# Citadel public documentation site

This is the canonical **product/API documentation source** for Citadel, built
with [Starlight](https://starlight.astro.build/) (Astro). It is for people
building **against** Citadel: clients, SDKs, the C ABI, the CLI, config,
transports, guides, quickstarts, and public references.

- Internal engineering docs live in `../docs/`: architecture, testing policy,
  AI collaboration, design research, and non-public engineering notes.
- Existing `../docs/features/` pages are legacy source material until migrated
  into this site or retired.
- This site documents only **implemented** behavior and clearly marks anything
  deferred.
- The source pages are Markdown/MDX under `src/content/docs/`, so humans,
  GitHub, and AI agents can read them directly without running Astro.

> Local-only. This site has **no** external hosting/deploy configuration by
> design — no Vercel/Netlify/Cloudflare/Pages/etc., no deploy CLIs, no env vars,
> no credentials. Build and preview locally only.

It is a Node sub-project **outside** the Cargo workspace. Do not commit
`node_modules/`, `dist/`, `.astro/`, or `public/rustdoc/`.

## Quick start (from the repo root)

The repo `Makefile` wraps the common tasks:

```bash
make docs-install   # install Node dependencies in website/
make docs-build      # build the site + generate rustdoc into public/rustdoc/
make docs-serve      # preview the built site locally
```

`make docs-serve` runs `npm run preview` and prints a local URL.

## Working directly in website/

```bash
npm install          # install dependencies
npm run dev          # live dev server at http://localhost:4321/
npm run build        # build the static site to ./dist/ (runs Pagefind search)
npm run preview      # preview the built ./dist/ locally
```

## Generated API reference

- **Rust API (rustdoc):** `cargo doc --no-deps --workspace` writes HTML to
  `target/doc/`. `make docs-build` copies it into `website/public/rustdoc/`
  (gitignored), so the built site serves it under `/rustdoc/`.
- **C ABI:** the source of truth is the committed header
  `../crates/citadel-client-ffi/include/citadel_client.h`. It is mirrored on the
  "C ABI" reference page; update that page when the header changes.

## Structure

```
src/
  content/
    docs/
      index.mdx              # landing page
      introduction.md
      quickstart.md
      concepts/              # transports, gateway, sessions, envelopes
      guides/                # web/native/Rust SDK/C ABI/engines
      reference/             # CLI, config, Rust SDK, C ABI, envelope, generated
      changelog.md
  content.config.ts
astro.config.mjs             # title, sidebar, dark mode + Pagefind (built in)
public/                      # static assets (favicon); rustdoc/ is generated
```

Dark mode and local search (Pagefind) ship with Starlight; search is wired
automatically during `npm run build`.
