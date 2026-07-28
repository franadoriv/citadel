---
title: Production TLS and reverse proxy
description: "Serve Citadel safely on the public internet: CA-issued certificates for QUIC/WebTransport and HTTPS/WSS through a reverse proxy."
---

Citadel has two public TLS boundaries:

- **QUIC** (`UDP 7351`) and **WebTransport** (`UDP 7353`) use the certificate
  configured in `[transport.tls]` directly.
- **HTTP and WebSocket** stay on loopback and are published as HTTPS/WSS by a
  reverse proxy. The built-in WebSocket listener is deliberately plain `ws://`.

Use a real DNS name such as `game.example.com`; the certificate's DNS name must
match the address clients connect to. Keep the dashboard private when possible.

## Configure Citadel

Store the PEM chain and private key outside `/opt/citadel`, with restrictive
permissions, then set both paths in `/opt/citadel/citadel.toml`:

```toml
[server]
public_addr = "game.example.com:7350"

[transport.tls]
certificate_file = "/etc/letsencrypt/live/game.example.com/fullchain.pem"
private_key_file = "/etc/letsencrypt/live/game.example.com/privkey.pem"

[transport.quic]
enabled = true
bind = "0.0.0.0:7351"

[transport.webtransport]
enabled = true
bind = "0.0.0.0:7353"

[transport.websocket]
enabled = true
bind = "127.0.0.1:7352"
```

Validate and restart after changing it:

```bash
sudo -u citadel /opt/citadel/citadel --config /opt/citadel/citadel.toml check
sudo systemctl restart citadel
```

The service account must be able to read the certificate and key. Citadel reads
them once at startup, so renewals require a restart. Open only UDP `7351` and
UDP `7353` in the firewall when the corresponding transports are enabled.

## Publish HTTPS and WSS with Caddy

[Caddy](https://caddyserver.com/) is a good default because it obtains and
renews public certificates automatically. Point DNS for `game.example.com` at
the server, install Caddy, then use this `/etc/caddy/Caddyfile`:

```
game.example.com {
    # HTTP health/status and the dashboard; protect the dashboard separately.
    reverse_proxy 127.0.0.1:7350

    # WebSocket upgrades are forwarded automatically by reverse_proxy.
    handle_path /socket* {
        reverse_proxy 127.0.0.1:7352
    }
}
```

Adapt the path to the client endpoint you expose. Caddy will serve `https://`
and `wss://` using the same hostname. Keep Citadel's HTTP/WebSocket binds on
`127.0.0.1` so they cannot bypass the proxy.

Do **not** put QUIC or WebTransport behind an ordinary TCP reverse proxy. They
are UDP protocols; clients must reach Citadel's configured UDP ports directly.
If your edge provider cannot forward UDP, offer WSS as the browser fallback.

## Checklist

- DNS resolves to the server and the certificate covers that exact hostname.
- TCP 80/443 reaches Caddy; UDP 7351/7353 reach Citadel only when required.
- `citadel check` succeeds and `systemctl status citadel` is healthy.
- A browser page loaded over HTTPS connects with `wss://`, never `ws://`.
- Default dashboard credentials have been changed and dashboard access is
  restricted to trusted operators.
