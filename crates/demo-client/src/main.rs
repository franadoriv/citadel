//! Native visual demo client for Citadel's QUIC transport.
//!
//! Renders a 2D scene with macroquad: a blue square you move with WASD/arrow
//! keys. Each frame the client sends its position over QUIC (an unreliable
//! datagram and a reliable stream) via `citadel-client`; the server echoes it
//! and the demo draws a green "datagram ghost" and an orange "stream ghost" at
//! the echoed positions, proving both QUIC paths work end to end.
//!
//! Network and state logic live in [`state`] and [`net`] (both testable and
//! macroquad-free); this file is only the render loop.
//!
//! Usage: `demo-client [SERVER_ADDR]` (default `127.0.0.1:7351`). The server
//! must run with `transport.quic.enabled = true`. No credentials are embedded;
//! the demo uses dev TLS that does not verify the server's self-signed cert.

mod net;
mod state;

use std::net::SocketAddr;

use macroquad::prelude::*;

use net::NetHandle;
use state::{Pos, WorldState};

/// World half-extent in world units; the play area is the square [-LIMIT, LIMIT].
const LIMIT: f32 = 9.0;
/// Movement speed in world units per second.
const SPEED: f32 = 6.0;
/// Pixels per world unit when drawing.
const SCALE: f32 = 28.0;

fn resolve_server_addr() -> SocketAddr {
    let arg = std::env::args().nth(1);
    let raw = arg
        .or_else(|| std::env::var("CITADEL_QUIC_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:7351".to_string());
    raw.parse().unwrap_or_else(|_| {
        // Fall back to the default if the argument is malformed.
        SocketAddr::from(([127, 0, 0, 1], 7351))
    })
}

/// Convert a world position to screen coordinates centered on the window.
fn to_screen(pos: Pos) -> (f32, f32) {
    let cx = screen_width() / 2.0;
    let cy = screen_height() / 2.0;
    (cx + pos.x * SCALE, cy + pos.y * SCALE)
}

fn draw_marker(pos: Pos, size: f32, color: Color, label: &str) {
    let (sx, sy) = to_screen(pos);
    draw_rectangle(sx - size / 2.0, sy - size / 2.0, size, size, color);
    draw_text(label, sx - size / 2.0, sy - size / 2.0 - 4.0, 16.0, color);
}

#[macroquad::main("Citadel QUIC Demo")]
async fn main() {
    let server_addr = resolve_server_addr();
    let net = NetHandle::spawn(server_addr, "localhost".to_string(), true);

    let mut world = WorldState::default();
    let mut status = format!("connecting to {server_addr} ...");

    loop {
        let dt = get_frame_time();

        // Input -> local movement.
        let mut dx = 0.0;
        let mut dy = 0.0;
        if is_key_down(KeyCode::W) || is_key_down(KeyCode::Up) {
            dy -= SPEED * dt;
        }
        if is_key_down(KeyCode::S) || is_key_down(KeyCode::Down) {
            dy += SPEED * dt;
        }
        if is_key_down(KeyCode::A) || is_key_down(KeyCode::Left) {
            dx -= SPEED * dt;
        }
        if is_key_down(KeyCode::D) || is_key_down(KeyCode::Right) {
            dx += SPEED * dt;
        }
        if dx != 0.0 || dy != 0.0 {
            world.move_local(dx, dy, LIMIT);
        }

        // Send the current position; apply relayed peer positions and status.
        net.send_position(world.local);
        while let Some(env) = net.try_recv_peer() {
            world.apply_relayed(&env);
        }
        if let Some(s) = net.try_recv_status() {
            status = s;
        }

        // Render.
        clear_background(Color::from_rgba(11, 14, 20, 255));
        // Play area border.
        let (bx0, by0) = to_screen(Pos::new(-LIMIT, -LIMIT));
        draw_rectangle_lines(
            bx0,
            by0,
            2.0 * LIMIT * SCALE,
            2.0 * LIMIT * SCALE,
            2.0,
            Color::from_rgba(42, 49, 64, 255),
        );

        // Other players relayed by the gateway, keyed by session id.
        for (peer_id, pos) in &world.peers {
            draw_marker(
                *pos,
                26.0,
                Color::from_rgba(91, 214, 111, 220),
                &format!("peer {peer_id}"),
            );
        }
        draw_marker(
            world.local,
            30.0,
            Color::from_rgba(42, 109, 244, 255),
            "you",
        );

        draw_text(&status, 12.0, 24.0, 22.0, WHITE);
        draw_text(
            "WASD / arrows to move. Blue = you, green = other players (relayed over QUIC).",
            12.0,
            screen_height() - 16.0,
            18.0,
            Color::from_rgba(200, 200, 200, 255),
        );

        next_frame().await;
    }
}
