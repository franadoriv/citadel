---
title: Organize multi-file Lua and Python game logic
description: Split a Citadel game server into local Lua or Python modules while preserving safe loading and predictable reloads.
---

Citadel starts a game from one entrypoint: `main.lua` for Lua or `main.py` for
Python. That entrypoint can register hooks and compose the rest of your game
from files under `[runtime] scripts_dir` (normally `game/`). Keep host-API
registration in the entrypoint or in a module it imports during initialization.

## Choose a layout

<Tabs syncKey="runtime-lang">
  <TabItem label="Lua">

  ```text
  game/
  ├── main.lua
  ├── systems/
  │   ├── combat.lua
  │   └── rewards.lua
  └── data/
      └── balance.lua
  ```

  `require` accepts a dotted module name. Each segment becomes a directory
  name, so `require("systems.combat")` loads `game/systems/combat.lua`.

  ```lua
  -- game/main.lua
  local combat = require("systems.combat")

  citadel.on_message(1, function(ctx, body)
    combat.apply_damage(ctx, 10)
  end)
  ```

  ```lua
  -- game/systems/combat.lua
  local M = {}

  function M.apply_damage(ctx, amount)
    citadel.log("damage=" .. amount .. " sender=" .. ctx.sender)
  end

  return M
  ```

  Lua modules run once per VM and their returned value is cached. Citadel's
  loader only accepts non-empty `[A-Za-z0-9_]` dotted segments. It rejects
  absolute paths, `..`, separators, `package.path`, native/C loaders, `io`, and
  `os`; `require` can read only Lua source contained by the game root.

  </TabItem>
  <TabItem label="Python">

  ```text
  game/
  ├── main.py
  └── systems/
      ├── __init__.py
      └── combat.py
  ```

  Citadel adds `game/` to CPython's `sys.path` before it evaluates `main.py`, so
  use ordinary Python imports for your local modules.

  ```python
  # game/main.py
  import citadel
  from systems.combat import apply_damage

  @citadel.on_message(1)
  def on_damage(ctx, body):
      apply_damage(ctx, 10)
  ```

  ```python
  # game/systems/combat.py
  def apply_damage(ctx, amount):
      # `citadel` can be imported here too when this module needs host APIs.
      print(f"damage={amount} sender={ctx.sender}")
  ```

  This is the trusted embedded CPython runtime, not a Python filesystem
  sandbox. It provides ordinary Python import behavior, including the standard
  library and packages available to the server process. Operate only code you
  trust and keep game-local imports under `game/` so their ownership is clear.

  </TabItem>
</Tabs>

## Reload behavior

With `[runtime] hot_reload = true`, Citadel watches the entrypoint
`main.lua` or `main.py`. A successful reload builds a fresh VM before replacing
the live one, so a syntax error or failing import leaves the current game logic
serving.

Lua rebuilds its scoped `require` cache. Python evicts modules loaded from the
game directory before executing fresh `main.py`. Consequently, both runtimes
pick up imported-module edits *when a reload occurs*.

Today, editing an imported `.lua` or `.py` file alone does not trigger the
watcher. After changing a dependency, also touch the entrypoint, use the
operator reload control, or restart the server. A future dependency-aware
watcher can remove that operational step without changing import semantics.

## JavaScript: scoped native ESM

QuickJS now evaluates `main.js` as an ESM entrypoint. Separate JavaScript game
logic into local `.js` modules beneath `game/`:

```text
game/
├── main.js
└── systems/
    └── combat.js
```

```js
// game/main.js
import { damage } from "./systems/combat.js";

citadel.on_message(1, (ctx) => {
  citadel.broadcast(2, String(damage(ctx.sender)), false);
});
```

```js
// game/systems/combat.js
export function damage(sender) {
  return Number(sender % 100n) + 10;
}
```

Only relative `/`-separated `.js` imports are accepted, and their canonical
target must stay below `game/`. Citadel rejects absolute paths, traversal that
escapes that root, bare package imports, Node/npm APIs, CommonJS, TypeScript,
and native modules. Every imported dependency is watched by hot reload; a bad
module replacement leaves the current VM serving.

## Reference pages

- [Lua `require` reference](/reference/server-sdk/lua-runtime/#require--scoped-module-loading)
- [Python runtime reference](/reference/server-sdk/python-runtime/)
- [JavaScript runtime reference](/reference/server-sdk/js-runtime/)
