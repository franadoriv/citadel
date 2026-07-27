---
title: Use shared static gameplay data
description: Load bounded JSON and CSV gameplay constants into Lua, Python, or JavaScript game logic at server initialization.
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

Static data is for versioned, operator-owned constants that both a game server
and its clients need: collision volumes, hitbox offsets, tuning values, and
rules. It is not a player-data store and it is not a general filesystem API.

1. Put the selected game-runtime entrypoint and shared data in separate
   directories. The example layout is deliberately simple; choose one entrypoint
   extension: `main.lua`, `main.py`, or `main.js`.

   ```text
   game/
  main.<ext>
   common/
     gameplay/
       collision.json
       attacks.csv
   citadel.toml
   ```

2. Configure the two roots independently. The data root must already exist;
   Citadel never creates or writes it. Choose a per-file limit suitable for
   small gameplay definitions, not for arbitrary content blobs.

   <Tabs syncKey="runtime-lang">
     <TabItem label="Lua">

   ```toml
   [runtime]
   language = "lua"
   scripts_dir = "./game"
   static_data_dir = "./common"
   static_data_max_file_bytes = 65536
   hot_reload = true
   hot_reload_poll_ms = 250
   ```

     </TabItem>
     <TabItem label="Python">

   ```toml
   [runtime]
   language = "python"
   scripts_dir = "./game"
   static_data_dir = "./common"
   static_data_max_file_bytes = 65536
   hot_reload = true
   hot_reload_poll_ms = 250
   ```

     </TabItem>
     <TabItem label="JavaScript">

   ```toml
   [runtime]
   language = "js"
   scripts_dir = "./game"
   static_data_dir = "./common"
   static_data_max_file_bytes = 65536
   hot_reload = true
   hot_reload_poll_ms = 250
   ```

     </TabItem>
   </Tabs>

   If another local process can edit server files, make `common/` read-only with
   your platform's filesystem permissions or mount it read-only. Citadel itself
   only reads this tree.

3. Load every data file while the entrypoint initializes and keep the returned
   values in runtime memory. Paths are always relative to `static_data_dir`, use
   `/` separators, and must end in the matching extension.

   <Tabs syncKey="runtime-lang">
     <TabItem label="Lua">

   ```lua
   local collision = citadel.static_data.load_json("gameplay/collision.json")
   local attacks = citadel.static_data.load_csv("gameplay/attacks.csv")

   local knight = collision.characters.knight.hitbox
   local balloon = collision.characters.balloon.hitbox

   citadel.on_message(80, function(ctx, body)
     -- Calculate distance from authoritative actor state. Clients may render
     -- the same data, but they never decide whether this hit is accepted.
     local allowed_cm = knight.radius_cm + balloon.radius_cm
     if #body <= allowed_cm then
       citadel.broadcast(81, "authoritative_hit", false)
     end
   end)
   ```

     </TabItem>
     <TabItem label="Python">

   ```python
   import citadel

   collision = citadel.static_data.load_json("gameplay/collision.json")
   attacks = citadel.static_data.load_csv("gameplay/attacks.csv")

   knight = collision["characters"]["knight"]["hitbox"]
   balloon = collision["characters"]["balloon"]["hitbox"]

   @citadel.on_message(80)
   def hit(ctx, body):
       allowed_cm = knight["radius_cm"] + balloon["radius_cm"]
       if len(body) <= allowed_cm:
           citadel.broadcast(81, b"authoritative_hit")
   ```

     </TabItem>
     <TabItem label="JavaScript">

   ```js
   const collision = citadel.static_data.load_json("gameplay/collision.json");
   const attacks = citadel.static_data.load_csv("gameplay/attacks.csv");

   const knight = collision.characters.knight.hitbox;
   const balloon = collision.characters.balloon.hitbox;

   citadel.on_message(80, (ctx, body) => {
     const allowedCm = knight.radius_cm + balloon.radius_cm;
     if (body.length <= allowedCm) {
       citadel.broadcast(81, "authoritative_hit");
     }
   });
   ```

     </TabItem>
   </Tabs>

   JSON must have an object or array at its root. CSV must be UTF-8 with a
   non-empty, unique header row and a consistent number of columns; returned CSV
   rows are tables keyed by header. `true`/`false` and finite numbers are
   converted, while other cells remain strings.

4. Run `citadel check --config citadel.toml` before starting the node. A missing
   root, bad configuration, absent data file, size-limit violation, malformed
   JSON/CSV, or invalid CSV/JSON schema reports a clear error during script
   initialization. Errors name only the relative requested path, never the
   server's data-root path.

5. Deploy the same versioned `common/` tree alongside each client for UI or
   presentation. Citadel does not serve these files to clients automatically;
   package them through your game's normal content pipeline. The server's
   in-memory copy remains authoritative for collision and balance validation.

6. With hot reload enabled, changing a data file successfully loaded during
   initialization causes Citadel to build a replacement VM and data catalog off
   the dispatch path. A fully valid replacement swaps atomically. If the new
   data, script, or registration is invalid, the prior VM and parsed catalog keep
   serving. In-VM state resets on a successful reload, so put durable state in
   the appropriate Citadel service rather than a global.

The static-data capability only exposes `citadel.static_data.load_json` and
`citadel.static_data.load_csv`; it never hands the script a data-root path,
directory, or raw file handle. Absolute paths, Windows drive paths, backslashes,
`.`/`..`, non-data extensions, and symbolic links that resolve outside the
configured root are denied. A loader call made after initialization can return
an already-cached file, but a cache miss is denied so message and tick handlers
cannot trigger filesystem I/O. This capability does not change the independent
trusted-tier permissions of a language runtime; use it instead of direct file
reads when game code needs this bounded, reload-aware catalog.

The runnable example is in `examples/static-data-game` in a source checkout.
See the individual [Lua](/reference/server-sdk/lua-runtime/#citadelstatic_dataload_json),
[Python](/reference/server-sdk/python-runtime/#citadelstatic_dataload_json), and
[JavaScript](/reference/server-sdk/js-runtime/#citadelstatic_dataload_json)
runtime references for the API contracts and the
[configuration reference](/reference/operations/configuration/#runtime) for all
runtime options.
