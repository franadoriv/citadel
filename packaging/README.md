# `packaging/` — tracked packaging templates (inputs, not output)

`packaging/` holds **tracked** files that the `bin-*` staging targets and
`package-windows` copy into a staged layout: quickstart READMEs and small
boilerplate such as the hello-world `main.lua`. These are hand-maintained
inputs, checked into git like any other source file.

```
packaging/
  server/
    scripts/main.lua   Boilerplate game logic staged into bin/server/scripts/
                        and the "server" Windows release package.
    README.txt         Quickstart copied to bin/server/README.txt.
  windows/
    README.md          Quickstart copied to the root of the Windows release zip.
    unity-README.md     Import instructions copied to the packaged Unity SDK.
```

## `packaging/` vs `bin/` vs `dist/`

Do not confuse this folder with the two git-ignored output directories it
feeds:

- **`packaging/`** (this folder) — tracked templates/inputs. Edit these when
  the quickstart instructions or boilerplate script need to change.
- **`bin/`** — git-ignored **local runnable staging**, produced by the
  `bin-*` make targets (`make bin-server`, `make bin-benchmark`,
  `make bin-client-<engine>`, `make bin-clients`, `make bin-all`, or the
  matching `.\make.ps1` targets). This is what a developer runs locally.
- **`dist/`** — git-ignored **versioned release packages** (zips), produced
  by `make package-windows` / `.\make.ps1 package-windows`.

Adding a new packaging template: put it under `packaging/<area>/`, then wire
it into the relevant `bin-*` and/or `package-windows` target in `Makefile`
and `make.ps1` so it gets copied into the staged output.
