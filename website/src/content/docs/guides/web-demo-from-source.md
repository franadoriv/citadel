---
title: Run the web demo from source
description: Build Citadel locally and run the two-browser-tab WebSocket relay demo for development or contribution.
---

This is the source-development path. Use it when you are changing Citadel,
working on its examples, or need the tracked two-tab browser relay demo. For
making a game server with a published build, start with
[Getting started](/quickstart/) instead.

## What you need

- Git;
- a recent stable [Rust toolchain](https://rustup.rs/);
- Python 3 to serve the local browser files;
- GNU Make on macOS/Linux, or PowerShell on Windows.

## Run the demo

```bash
git clone https://github.com/franadoriv/citadel.git
cd citadel
make demo-web
```

On Windows PowerShell:

```powershell
git clone https://github.com/franadoriv/citadel.git
cd citadel
.\make.ps1 demo-web
```

The first Rust build can take a few minutes. When the terminal prints the local
URL, open <http://127.0.0.1:8000/> in two browser tabs. Move one blue cube with
WASD, arrow keys, or dragging; the other tab should render it.

The demo is a relay lesson, not a cheat-proof game. Build
[Knights vs Monsters](/tutorials/knights-vs-monsters/) to add authoritative
server rules.

## If it fails

- If Python is missing, install Python 3 and retry.
- If port 7352 or 8000 is busy, stop the process using it and rerun the command.
- If the build fails, run `rustup update`, open a new terminal, and retry.
