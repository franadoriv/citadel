---
title: Generated API docs
description: How to regenerate and view the Rust API docs and the C header locally.
---

The hand-written reference here is paired with an **auto-generated** API
reference. Generated output is never committed; you produce it locally.

## Rust API (rustdoc)

Generate docs for every workspace crate (server + client crates) without
dependency docs:

```bash
cargo doc --no-deps --workspace
```

This writes HTML to `target/doc/`. To browse it from this site, copy it into the
site's (gitignored) public folder and link it; the `docs-build` Makefile target
does this for you:

```bash
make docs-build
```

That target runs `cargo doc --no-deps --workspace`, then copies `target/doc` into
`website/public/rustdoc/`. Starlight serves `public/` at the site root, so after a
site build the rustdoc is available under `/rustdoc/`. The `website/public/rustdoc/`
path is gitignored and never committed.

Open the generated entry points (after `make docs-build` and `npm run build`):

- `citadel` (server library)
- `citadel_client` (Rust SDK)
- `citadel_wire` (wire format)
- `citadel_client_ffi` (C ABI crate)

You can also open `target/doc/citadel_client/index.html` directly in a browser
without the site.

## C ABI header

The C ABI's source of truth is the committed cbindgen header:

```
crates/citadel-client-ffi/include/citadel_client.h
```

It is mirrored on the [C ABI reference](/reference/client-sdk/c-abi/) page. It is also
regenerated on `cargo build -p citadel-client-ffi` (best-effort; a committed copy
is always provided). When the header changes, update the C ABI reference page to
match it.

## Regeneration summary

| Reference | Source of truth | Regenerate with |
| --- | --- | --- |
| Rust API | crate doc comments | `cargo doc --no-deps --workspace` (via `make docs-build`) |
| C ABI | `include/citadel_client.h` | `cargo build -p citadel-client-ffi`; mirror into the C ABI page |
| Config / CLI | `src/config/`, `src/cli.rs` | hand-curated from the source |
| Envelope / protocol | `crates/citadel-wire` | hand-curated from the source |

:::note[Local only]
All commands here are local. There is no external publishing or hosting step.
:::
