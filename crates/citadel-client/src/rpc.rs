use citadel_wire::Envelope;
use citadel_wire::protocol::{KIND_RPC_RESPONSE, decode_rpc_response};

use crate::{ClientError, ClientResult};

/// Routes the single inbound stream while one RPC is in flight.
///
/// Every envelope that is not the matching response is synchronously handed to
/// the caller-owned pump; this type never buffers or drops it.
pub(crate) struct RpcResponsePump<'a, F> {
    request_id: u64,
    on_envelope: &'a mut F,
}

impl<'a, F> RpcResponsePump<'a, F>
where
    F: FnMut(Envelope) -> ClientResult<()>,
{
    pub(crate) fn new(request_id: u64, on_envelope: &'a mut F) -> Self {
        Self {
            request_id,
            on_envelope,
        }
    }

    pub(crate) fn handle(&mut self, envelope: Envelope) -> ClientResult<Option<Vec<u8>>> {
        if envelope.kind == KIND_RPC_RESPONSE
            && let Some(response) = decode_rpc_response(&envelope.body)
            && response.request_id == self.request_id
        {
            if response.is_ok() {
                return Ok(Some(response.payload.to_vec()));
            }
            return Err(ClientError::Rpc {
                request_id: self.request_id,
                message: String::from_utf8_lossy(response.payload).into_owned(),
            });
        }
        (self.on_envelope)(envelope)?;
        Ok(None)
    }

    /// Route one already-drained transport batch without losing its tail.
    ///
    /// The first correlated response is retained locally while all later
    /// envelopes are synchronously delivered to the caller-owned pump. This is
    /// deliberately the same `Vec<Envelope>` seam used by QUIC `recv_uni`.
    pub(crate) fn handle_batch(
        &mut self,
        envelopes: Vec<Envelope>,
    ) -> ClientResult<Option<Vec<u8>>> {
        let mut response = None;
        let mut routing_error = None;

        for envelope in envelopes {
            if response.is_none()
                && envelope.kind == KIND_RPC_RESPONSE
                && let Some(decoded) = decode_rpc_response(&envelope.body)
                && decoded.request_id == self.request_id
            {
                response = Some(if decoded.is_ok() {
                    Ok(decoded.payload.to_vec())
                } else {
                    Err(ClientError::Rpc {
                        request_id: self.request_id,
                        message: String::from_utf8_lossy(decoded.payload).into_owned(),
                    })
                });
            } else if let Err(error) = (self.on_envelope)(envelope)
                && routing_error.is_none()
            {
                routing_error = Some(error);
            }
        }

        if let Some(error) = routing_error {
            return Err(error);
        }
        response.transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use citadel_wire::protocol::{KIND_CHAT_EVENT, KIND_RPC_RESPONSE, encode_rpc_response};

    use super::*;
    use crate::ClientResult;

    #[test]
    fn chat_event_is_delivered_before_the_correlated_rpc_response() {
        let delivered = RefCell::new(Vec::new());
        let mut callback = |envelope: Envelope| -> ClientResult<()> {
            delivered.borrow_mut().push(envelope.kind);
            Ok(())
        };
        let mut pump = RpcResponsePump::new(9, &mut callback);

        assert!(
            pump.handle(Envelope::new(KIND_CHAT_EVENT, b"event".to_vec()))
                .expect("chat event callback")
                .is_none()
        );
        assert_eq!(*delivered.borrow(), vec![KIND_CHAT_EVENT]);

        let response = Envelope::new(KIND_RPC_RESPONSE, encode_rpc_response(9, 0, b"reply"));
        assert_eq!(
            pump.handle(response).expect("RPC response"),
            Some(b"reply".to_vec())
        );
        assert_eq!(*delivered.borrow(), vec![KIND_CHAT_EVENT]);
    }

    #[test]
    fn drained_batch_routes_every_envelope_after_a_matching_success() {
        let delivered = RefCell::new(Vec::new());
        let mut callback = |envelope: Envelope| -> ClientResult<()> {
            delivered
                .borrow_mut()
                .push((envelope.kind, envelope.body.to_vec()));
            Ok(())
        };
        let mut pump = RpcResponsePump::new(9, &mut callback);
        let batch = vec![
            Envelope::new(KIND_CHAT_EVENT, b"before".to_vec()),
            Envelope::new(KIND_RPC_RESPONSE, encode_rpc_response(9, 0, b"reply")),
            Envelope::new(KIND_CHAT_EVENT, b"after".to_vec()),
            Envelope::new(KIND_RPC_RESPONSE, encode_rpc_response(10, 0, b"other")),
            Envelope::new(KIND_RPC_RESPONSE, b"malformed".to_vec()),
        ];

        assert_eq!(
            pump.handle_batch(batch).expect("batch routing"),
            Some(b"reply".to_vec())
        );
        assert_eq!(
            *delivered.borrow(),
            vec![
                (KIND_CHAT_EVENT, b"before".to_vec()),
                (KIND_CHAT_EVENT, b"after".to_vec()),
                (KIND_RPC_RESPONSE, encode_rpc_response(10, 0, b"other")),
                (KIND_RPC_RESPONSE, b"malformed".to_vec()),
            ]
        );
    }

    #[test]
    fn drained_batch_routes_the_tail_after_a_matching_rpc_error() {
        let delivered = RefCell::new(Vec::new());
        let mut callback = |envelope: Envelope| -> ClientResult<()> {
            delivered
                .borrow_mut()
                .push((envelope.kind, envelope.body.to_vec()));
            Ok(())
        };
        let mut pump = RpcResponsePump::new(9, &mut callback);
        let batch = vec![
            Envelope::new(KIND_RPC_RESPONSE, encode_rpc_response(9, 1, b"denied")),
            Envelope::new(KIND_CHAT_EVENT, b"after-error".to_vec()),
            Envelope::new(KIND_RPC_RESPONSE, b"malformed".to_vec()),
        ];

        let error = pump.handle_batch(batch).expect_err("correlated RPC error");
        assert!(matches!(error, ClientError::Rpc { request_id: 9, .. }));
        assert_eq!(
            *delivered.borrow(),
            vec![
                (KIND_CHAT_EVENT, b"after-error".to_vec()),
                (KIND_RPC_RESPONSE, b"malformed".to_vec()),
            ]
        );
    }
}
