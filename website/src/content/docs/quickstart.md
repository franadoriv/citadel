---
title: "Quickstart: two players, one shared world"
description: Clone Citadel, start the local WebSocket demo with one command, and see two browser players move in the same world.
---

Your goal is simple: open two browser tabs, move a player in one tab, and watch
the other tab render that player. The first build can take a few minutes; the
multiplayer test itself takes less than one.

## Before you start

Install:

- [Git](https://git-scm.com/) to clone the repository;
- a recent stable [Rust toolchain](https://rustup.rs/);
- Python 3 to serve the local browser files;
- any modern browser. The golden path uses WebSocket, so Chromium is not
  required.

On Windows, use the tracked PowerShell wrapper below; GNU Make is not required.

## 1. Get Citadel

```bash
git clone <repository-url>
cd citadel
```

You should now be in the repository root, beside `Cargo.toml`, `citadel.toml`,
and `game/`.

## 2. Start the server and browser demo

### macOS or Linux

```bash
make demo-web
```

### Windows PowerShell

```powershell
.\make.ps1 demo-web
```

The command builds Citadel, starts the local server, then serves the browser
demo. The first Rust build is the slow part. Wait for output similar to:

```text
Server up. Open http://127.0.0.1:8000/ in your browser
Open two tabs to see the relay; WebSocket connects with no setup.
```

:::tip[Checkpoint]
Keep this terminal open. The demo server and web server stop when you press
`Ctrl-C`.
:::

## 3. Open two players

Open <http://127.0.0.1:8000/> in two browser tabs.

1. Wait until both tabs show a connected WebSocket state.
2. Move the blue cube in the first tab with WASD, the arrow keys, or dragging.
3. Look at the second tab. It should render the other participant moving.
4. Move in the second tab and confirm the first tab sees it too.

That is the win: two independent clients are sharing realtime state through
Citadel.

## What just happened

The demo hides the networking ceremony so you can see the result first:

1. Each browser opened a WebSocket connection to Citadel.
2. Its first reliable message requested a guest handshake.
3. Citadel accepted the guest and assigned a temporary `ParticipantId` for that
   connection.
4. A moving tab sent `KIND_POSITION`, the small position message used by the
   tracked demo.
5. Citadel relayed `KIND_PEER_POSITION` to the other participant and tagged it
   with the sender's `ParticipantId`.
6. The receiving tab used that tag to move the correct peer cube.

The tracked demo is a relay lesson, not a cheat-proof movement system. In a real
game, your server-side script validates the move before it shares the accepted
state. The [Knights vs Monsters tutorial](/tutorials/knights-vs-monsters/) adds
that authoritative rule layer.

## If something goes wrong

### `python3` is not found

Install Python 3. On Windows, the PowerShell wrapper also checks common `python`
and Python launcher commands.

### Port 7352 or 8000 is already in use

Stop the other local server, then run the demo command again. Citadel uses 7352
for this WebSocket path and Python uses 8000 for the page.

### The first build fails

Run `rustup update`, open a new terminal, and retry. If Cargo names a missing
system dependency, follow the platform-specific package it reports rather than
ignoring the first error.

### Both tabs connect but no peer moves

Refresh both tabs after the terminal prints that the server is ready. Keep both
tabs on the same `http://127.0.0.1:8000/` origin and check the browser console for
the first connection error.

## Choose the next quest

- Add authoritative combat in [Build Knights vs Monsters](/tutorials/knights-vs-monsters/).
- Connect a visual browser game with [Three.js + `@citadel/client`](/guides/web-client/).
- Bring an existing project through the [Unity, Unreal, or Godot SDK](/guides/install-client-sdk/).
- Learn why the server has the final say in [Game logic & server authority](/concepts/game-logic/).
- Explore exact startup options in the [CLI reference](/reference/operations/cli/).
