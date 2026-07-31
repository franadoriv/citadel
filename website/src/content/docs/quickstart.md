---
title: "Getting started: your first server-side game rule"
description: Download Citadel, run a ready-made server, and change server-authoritative gameplay without cloning or compiling anything.
---

Citadel is for writing the rules of your game, not for assembling a Rust build
environment. Start with a published server release: no Git checkout, Rust,
Cargo, or source compilation is required.

In a few minutes you will run a local server and change the Lua game logic that
it owns. Save the file and Citadel reloads it live.

## Before you start

You need a 64-bit Windows or Linux machine and a text editor. Download the
matching Citadel server ZIP from [GitHub Releases](https://github.com/franadoriv/citadel/releases).
The archive name tells you which one to choose:

| Host | Archive |
| --- | --- |
| Windows 64-bit | `citadel-windows-x86_64-v{version}.zip` |
| Linux x86_64 / AMD64 | `citadel-linux-x86_64-musl-v{version}.zip` |
| Linux ARM64 / AArch64 | `citadel-linux-aarch64-musl-v{version}.zip` |

Extract the ZIP somewhere you can edit. The included `README` has checksum
verification and platform details if you need them.

:::note[Developing Citadel is a different path]
Building Citadel itself from source is for contributors, unsupported platforms,
and internal development. See [Run the web demo from source](/guides/web-demo-from-source/)
when that is what you need.
:::

## 1. Start your server

Open a terminal in the extracted folder and run:

```powershell
# Windows PowerShell
.\citadel.exe
```

```bash
# Linux
./citadel
```

On first run, Citadel creates `data.sqlite`, applies migrations, loads
`scripts/main.lua`, and starts its local HTTP and realtime listeners. Keep this
terminal open.

Open <http://127.0.0.1:7350/dashboard> in a browser. That is your local admin
dashboard; seeing it confirms your game server is alive.

## 2. Meet the game layer

Open `scripts/main.lua` in your editor. It is your server-side gameplay file.
It already contains lifecycle hooks, a message handler, an RPC, and a server
tick. Citadel, not a player’s client, runs this code.

Find this line near the top:

```lua
citadel.log("game logic loaded", "info")
```

Change the message to something that names your game, then save the file:

```lua
citadel.log("Moonlit Arena rules loaded", "info")
```

Watch the terminal. Citadel reloads a valid Lua edit without restarting the
server. If an edit has a syntax error, it keeps the previous working rules
instead of taking your game down.

That is the core loop: **write server gameplay → save → Citadel applies the new
rules**.

## 3. Make the server decide a rule

The shipped script includes a position-message relay so a client can share
movement. The server owns the decision about what is sent. For example, its
message handler receives the player’s request, tags it with the real sender,
and broadcasts the accepted result:

```lua
citadel.on_message(KIND_POSITION, function(ctx, body)
    local tagged = string.pack(">I8", ctx.sender) .. body
    citadel.broadcast(KIND_PEER_POSITION, tagged, true)
end)
```

As your game grows, this is where you validate moves, reject impossible attacks,
update monster health, award rewards, and send the state the clients should
render. The client asks; your server game logic decides.

## Where next?

- Build a small authoritative game in [Knights vs Monsters](/tutorials/knights-vs-monsters/).
- Connect a browser game with [Three.js + `@citadel/client`](/guides/web-client/).
- Bring an existing project through the [Unity, Unreal, or Godot SDK](/guides/install-client-sdk/).
- Learn the model behind the rule in [Game logic & server authority](/concepts/game-logic/).
- Need a source checkout? [Run the web demo from source](/guides/web-demo-from-source/).
