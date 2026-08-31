//! Realtime gateway: transport-agnostic session registry and message routing.
//!
//! This module realizes the single-node, in-memory subset of the routing design
//! (see `website/src/content/docs/guides/distributed-matchmaker.md`): a [`SessionRegistry`]
//! of active connections and a [`Gateway`] that relays application messages
//! between sessions. It depends only on the transport abstraction's `Envelope`
//! and `Delivery` types plus an abstract outbound `mpsc` sink, never on a
//! concrete transport.
//!
//! Step 1 scope: one global room; positions relay to all other sessions. Full
//! presence/streams, matches, and inter-node routing are future work.

pub mod auth;
pub mod chat_presence;
pub mod diagnostics;
pub mod gateway;
pub mod identity;
pub mod netpeer;
pub mod registry;
pub mod reload;
pub mod rooms;
pub mod tick;
pub mod transform;

pub use auth::{AuthOutcome, Authenticator, PresentedCredential, RejectReason};
pub use chat_presence::{ChatJoin, ChatLeave, ChatPresenceRegistry, ChatSubscription};
pub use diagnostics::{
    LagCaptureError, LagCaptureFlush, LagCaptureManager, LagCaptureParticipantState,
    LagCaptureParticipantStatus, LagCaptureStart, LagCaptureStatus,
};
pub use gateway::{
    DomainRpcServices, Gateway, Handshake, InboundMessageMetadata, KIND_AUTH, KIND_AUTH_RESULT,
    KIND_PEER_POSITION, KIND_POSITION, shared,
};
pub use identity::{IdentityLifecycle, Presence, PresenceState, ResumeResult, ResumeSecret};
pub use registry::{
    LatestOutboundReceiver, Outbound, ParticipantId, ParticipantIdGen, ParticipantIdentity,
    SessionHandle, SessionRegistry,
};
pub use reload::LuaReloadService;
pub use rooms::{BridgeMode, JoinError, RoomId, RoomLabel, RoomRegistry, RoomSnapshot};
pub use tick::{
    ChatDeliveryDispatchService, ChatPresenceRenewalService, GameplayClock, GameplayClockSnapshot,
    LuaTickService, MatchmakerTickService, ReconnectGraceExpiryService, TransformTickService,
};
pub use transform::{TransformHub, TransformHubConfig};
