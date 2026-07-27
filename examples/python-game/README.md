# Python Game Example

This directory mirrors the repo's default Lua sample without changing the
default `./game` scripts directory. The default sample still uses
`game/main.lua`; this example is selected explicitly.

Use a Python-enabled build and point the runtime at this directory:

```toml
[runtime]
enabled = true
language = "python"
scripts_dir = "./examples/python-game"
```

Then run:

```bash
cargo run --features runtime-python -- --config citadel-python.toml
```

For packaged releases, use the Python bundle target so the server ships with a
matching CPython runtime and standard library.
