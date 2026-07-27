# Citadel developer Makefile.
#
# Convenience targets for local development and running the realtime demos
# without juggling several terminals by hand. All demo targets are
# local-only and use the tracked dev config at examples/configs/demo.toml
# (QUIC :7351, WebSocket :7352, WebTransport :7353).
#
# Quick start:
#   make demo-web      # server + web demo (open the printed URL in a browser)
#   make demo-native   # server + one native QUIC client
#   make demo-native2  # server + two native clients (see the relay locally)
#   make benchmark-serve # combat benchmark: build, serve, and open the browser
#
# Local runnable staging (`bin-*` family): everything a developer runs
# locally is staged under git-ignored bin/, never dist/ (dist/ is reserved
# for versioned release zips via `package-windows`):
#   make bin-server          # runnable server: exe + config + scripts/ + maps/
#   make bin-benchmark        # combat benchmark package (server + Lua + HTML + JS)
#   make bin-client-<engine>  # copy-into-project SDK source (unity/unreal/godot/js/rust)
#   make bin-clients          # all five client SDKs
#   make bin-all              # bin-server + bin-benchmark + bin-clients
# Windows users can also run `make <target>` via the make.bat forwarder to
# make.ps1, or `.\make.ps1 <target>` directly.
#
# Run `make help` for the full list.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Tunables (override on the command line, e.g. `make demo-web WEB_PORT=9000`).
CONFIG    ?= examples/configs/demo.toml
WEB_DIR   ?= examples/web-demo
WEB_PORT  ?= 8000
QUIC_ADDR ?= 127.0.0.1:7351
BENCHMARK_DIR ?= $(BIN_DIR)/benchmark
BENCHMARK_WEB_PORT ?= 8080
# Seconds to wait for the server to bind before launching a client.
WAIT      ?= 3
# Public documentation site (Node sub-project, outside the Cargo workspace).
DOCS_DIR  ?= website
# Unity SDK: where the built native plugin (cdylib) is installed (git-ignored).
UNITY_PLUGIN_DIR ?= clients/unity/Plugins/x86_64
UNITY_MACOS_PLUGIN_DIR ?= clients/unity/Plugins/macOS
# Local Postgres for the persistence layer.A throwaway container;
# not for production. Override any tunable on the command line.
PG_IMAGE     ?= postgres:16-alpine
PG_CONTAINER ?= citadel-postgres
PG_PORT      ?= 5432
PG_USER      ?= citadel
PG_PASSWORD  ?= citadel
PG_DB        ?= citadel
DATABASE_URL ?= postgres://$(PG_USER):$(PG_PASSWORD)@localhost:$(PG_PORT)/$(PG_DB)

.PHONY: help build check fmt clippy test clean \
        server web native demo-web demo-native demo-native2 \
        docs-install docs-build docs-serve unity-plugin \
        db-up db-down db-migrate package-windows package-windows-python package-macos \
        package-client-unity package-client-unreal package-client-godot package-client-godot-web package-clients-windows \
        package-client-unity-macos package-client-unreal-macos package-client-godot-macos package-clients-macos \
        bin-server bin-server-python bin-benchmark benchmark-serve \
        bin-client-unity bin-client-unreal bin-client-godot bin-client-godot-web bin-client-js bin-client-rust \
        bin-clients bin-all

# --- Release packaging ----------------------------------------------------
# The release version comes from the workspace/binary Cargo.toml `version`
# (the developer bumps it per milestone). Both this Makefile target and the
# make.ps1 `package-windows` target stage the identical layout, so CI and local
# packaging share one definition. `DIST_DIR` is git-ignored build output.
DIST_DIR ?= dist
BIN_DIR  ?= bin
# Copy-into-project client SDK staging (see docs/architecture/client-sdk-layout.md).
CLIENTS_DIR ?= $(BIN_DIR)/clients
VERSION  := $(shell grep -m1 -E '^version[[:space:]]*=' Cargo.toml | sed -E 's/.*"([^"]+)".*/\1/')
# macOS packages are built natively on each architecture's runner. GitHub
# Actions uses an Apple Silicon runner for aarch64 and an Intel runner for x86_64;
# avoid cross-compiling Godot extensions because its toolchain must match the
# target SDK. Override only when the active toolchain really builds that arch.
MACOS_ARCH ?= $(shell uname -m | sed -e 's/^arm64$$/aarch64/')
MACOS_SIGN_IDENTITY ?=
MACOS_NOTARY_PROFILE ?=
MACOS_PYTHON ?= python3
MACOS_GODOT_VENV ?= $(CURDIR)/target/godot-build-venv
MACOS_GODOT_PYTHON = $(MACOS_GODOT_VENV)/bin/python
MACOS_DEPLOYMENT_TARGET ?= 15.0
MACOS_GODOT_ARCH = $(if $(filter aarch64,$(MACOS_ARCH)),arm64,$(MACOS_ARCH))

help: ## Show this help
	@echo "Citadel — make targets:"
	@echo
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Demo config: $(CONFIG)  (QUIC :7351  WebSocket :7352  WebTransport :7353)"
	@echo "Tip: WebSocket works with no setup; WebTransport needs the cert hash"
	@echo "     printed in the server log (see examples/web-demo/README.md)."

# --- Build / verify -------------------------------------------------------

build: ## Build the whole workspace
	cargo build --workspace

check: ## Run the canonical check (fmt, clippy, tests, docs)
	bash scripts/check.sh

fmt: ## Format the workspace
	cargo fmt

clippy: ## Lint with warnings denied
	cargo clippy --all-targets --all-features --workspace -- -D warnings

test: ## Run the workspace test suite
	cargo test --workspace

clean: ## Remove build artifacts
	cargo clean

# --- Single processes (each runs in the foreground) -----------------------

server: ## Run only the server (all transports enabled)
	cargo run -- --config $(CONFIG) serve

web: ## Serve only the web demo static files (assumes a server is running)
	@echo ">> Web demo at http://127.0.0.1:$(WEB_PORT)/"
	python3 -m http.server $(WEB_PORT) --directory $(WEB_DIR)

native: ## Run only one native QUIC client (assumes a server is running)
	cargo run -p demo-client -- $(QUIC_ADDR)

# --- Combined demos (start the server, then a client; Ctrl-C stops all) ---

demo-web: build ## Server + web demo static server (open the printed URL)
	@cargo run -- --config $(CONFIG) serve & SRV=$$!; \
	trap 'kill $$SRV 2>/dev/null' EXIT INT TERM; \
	sleep $(WAIT); \
	echo ">> Server up. Open http://127.0.0.1:$(WEB_PORT)/ in your browser"; \
	echo ">> (open two tabs to see the relay; WebSocket connects with no setup)"; \
	python3 -m http.server $(WEB_PORT) --directory $(WEB_DIR)

demo-native: build ## Server + one native QUIC client
	@cargo run -- --config $(CONFIG) serve & SRV=$$!; \
	trap 'kill $$SRV 2>/dev/null' EXIT INT TERM; \
	sleep $(WAIT); \
	echo ">> Server up. Launching native client ($(QUIC_ADDR))"; \
	cargo run -p demo-client -- $(QUIC_ADDR)

demo-native2: build ## Server + two native clients (move one, watch the other)
	@cargo run -- --config $(CONFIG) serve & SRV=$$!; \
	cargo run -p demo-client -- $(QUIC_ADDR) & C1=$$!; \
	trap 'kill $$SRV $$C1 2>/dev/null' EXIT INT TERM; \
	sleep $(WAIT); \
	echo ">> Server + one client up. Launching the second client"; \
	cargo run -p demo-client -- $(QUIC_ADDR)

# --- Unity SDK native plugin ----------------------------------------------

unity-plugin: ## Build the C ABI cdylib and install it into the Unity SDK
	cargo build --release -p citadel-client-ffi
	@if [ -f target/release/citadel_client_ffi.dll ]; then \
		mkdir -p $(UNITY_PLUGIN_DIR); \
		cp target/release/citadel_client_ffi.dll $(UNITY_PLUGIN_DIR)/; \
		echo ">> Installed citadel_client_ffi.dll -> $(UNITY_PLUGIN_DIR)/"; \
	elif [ -f target/release/libcitadel_client_ffi.dylib ]; then \
		mkdir -p $(UNITY_MACOS_PLUGIN_DIR); \
		cp target/release/libcitadel_client_ffi.dylib $(UNITY_MACOS_PLUGIN_DIR)/; \
		echo ">> Installed libcitadel_client_ffi.dylib -> $(UNITY_MACOS_PLUGIN_DIR)/"; \
	elif [ -f target/release/libcitadel_client_ffi.so ]; then \
		mkdir -p $(UNITY_PLUGIN_DIR); \
		cp target/release/libcitadel_client_ffi.so $(UNITY_PLUGIN_DIR)/; \
		echo ">> Installed libcitadel_client_ffi.so -> $(UNITY_PLUGIN_DIR)/"; \
	else \
		echo "!! No cdylib found under target/release/"; exit 1; \
	fi

# --- Windows release package ----------------------------------------------
# Build the server + Unity plugin DLL, stage the release layout, and zip it as
# citadel-windows-x86_64-v{version}.zip. This is the shared definition the
# release CI reuses (windows-latest calls make.ps1 package-windows) and the
# local verification path. Windows-first; a macOS/Linux target can be added
# alongside without changing this one.
package-windows: ## Stage + zip the Windows release ($(DIST_DIR)/citadel-windows-x86_64-v{version}.zip)
	@echo ">> Packaging Citadel v$(VERSION) for windows-x86_64"
	cargo build --release
	cargo build --release -p citadel-client-ffi
	$(eval PKG_NAME := citadel-windows-x86_64-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/clients/unity/Citadel"
	@mkdir -p "$(PKG_STAGE)/clients/unity/Demo"
	@mkdir -p "$(PKG_STAGE)/clients/unity/Plugins/x86_64"
	cp target/release/citadel.exe "$(PKG_STAGE)/citadel.exe"
	cp citadel.toml "$(PKG_STAGE)/citadel.toml"
	cp packaging/windows/README.md "$(PKG_STAGE)/README.md"
	cp clients/unity/Citadel/*.cs "$(PKG_STAGE)/clients/unity/Citadel/"
	cp clients/unity/Demo/*.cs "$(PKG_STAGE)/clients/unity/Demo/"
	cp target/release/citadel_client_ffi.dll "$(PKG_STAGE)/clients/unity/Plugins/x86_64/"
	cp packaging/windows/unity-README.md "$(PKG_STAGE)/clients/unity/README.md"
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

# --- macOS release package -------------------------------------------------
# Build the standalone server + Unity plugin for the active native macOS
# architecture. The release workflow runs this target once on Apple Silicon and
# once on Intel, yielding distinct, architecture-correct archives.
package-macos: ## Stage + zip the macOS release for the native arch ($(DIST_DIR)/citadel-macos-<arch>-v{version}.zip)
	@case "$(MACOS_ARCH)" in aarch64|x86_64) ;; *) echo "!! Unsupported macOS architecture: $(MACOS_ARCH)"; exit 1 ;; esac
	@echo ">> Packaging Citadel v$(VERSION) for macos-$(MACOS_ARCH)"
	MACOSX_DEPLOYMENT_TARGET="$(MACOS_DEPLOYMENT_TARGET)" cargo build --release
	MACOSX_DEPLOYMENT_TARGET="$(MACOS_DEPLOYMENT_TARGET)" cargo build --release -p citadel-client-ffi
	$(eval PKG_NAME := citadel-macos-$(MACOS_ARCH)-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/clients/unity/Citadel" "$(PKG_STAGE)/clients/unity/Demo" "$(PKG_STAGE)/clients/unity/Plugins/macOS"
	cp target/release/citadel "$(PKG_STAGE)/citadel"
	cp citadel.toml "$(PKG_STAGE)/citadel.toml"
	cp packaging/macos/README.md "$(PKG_STAGE)/README.md"
	cp clients/unity/Citadel/*.cs "$(PKG_STAGE)/clients/unity/Citadel/"
	cp clients/unity/Demo/*.cs "$(PKG_STAGE)/clients/unity/Demo/"
	cp target/release/libcitadel_client_ffi.dylib "$(PKG_STAGE)/clients/unity/Plugins/macOS/"
	cp packaging/macos/unity-README.md "$(PKG_STAGE)/clients/unity/README.md"
	@if [ -n "$(MACOS_SIGN_IDENTITY)" ]; then \
		bash scripts/sign-macos-artifacts.sh "$(PKG_STAGE)" "$(MACOS_SIGN_IDENTITY)"; \
	else \
		echo ">> macOS package is unsigned (set MACOS_SIGN_IDENTITY for a release build)"; \
	fi
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@if [ -n "$(MACOS_NOTARY_PROFILE)" ]; then \
		if [ -z "$(MACOS_SIGN_IDENTITY)" ]; then echo "!! MACOS_NOTARY_PROFILE requires MACOS_SIGN_IDENTITY"; exit 1; fi; \
		bash scripts/notarize-macos-archive.sh "$(DIST_DIR)/$(PKG_NAME).zip" "$(MACOS_NOTARY_PROFILE)"; \
	fi
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

bin-server: ## Stage a ready-to-run server at $(BIN_DIR)/server (exe + config + scripts/main.lua + empty maps/)
	@echo ">> Staging runnable server at $(BIN_DIR)/server"
	cargo build --release
	@rm -rf "$(BIN_DIR)/server"
	@mkdir -p "$(BIN_DIR)/server/scripts"
	@mkdir -p "$(BIN_DIR)/server/maps"
	@if [ -f target/release/citadel.exe ]; then \
		cp target/release/citadel.exe "$(BIN_DIR)/server/citadel.exe"; \
	else \
		cp target/release/citadel "$(BIN_DIR)/server/citadel"; \
	fi
	sed 's|scripts_dir = "./game"|scripts_dir = "./scripts"|' citadel.toml > "$(BIN_DIR)/server/citadel.toml"
	cp packaging/server/scripts/main.lua "$(BIN_DIR)/server/scripts/main.lua"
	cp packaging/server/README.txt "$(BIN_DIR)/server/README.txt"
	@echo ">> Ready: $(BIN_DIR)/server (run: cd $(BIN_DIR)/server && ./citadel serve, or citadel.exe on Windows)"

bin-server-python: ## Stage a Python-enabled server at $(BIN_DIR)/server-python (bundled CPython + scripts/main.py)
	@echo ">> Staging Python-enabled runnable server at $(BIN_DIR)/server-python"
	@source scripts/python-runtime-env.sh; export CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2; cargo build --release --features runtime-python
	@rm -rf "$(BIN_DIR)/server-python"
	@mkdir -p "$(BIN_DIR)/server-python/scripts"
	@mkdir -p "$(BIN_DIR)/server-python/maps"
	cp target/release/citadel.exe "$(BIN_DIR)/server-python/citadel.exe"
	sed -e 's|# language = "lua"|language = "python"|' \
		-e 's|scripts_dir = "./game"|scripts_dir = "./scripts"|' \
		citadel.toml > "$(BIN_DIR)/server-python/citadel.toml"
	cp packaging/server/scripts/main.py "$(BIN_DIR)/server-python/scripts/main.py"
	cp packaging/server/README-python.txt "$(BIN_DIR)/server-python/README.txt"
	bash scripts/stage-python-bundle.sh "$(BIN_DIR)/server-python"
	bash scripts/smoke-python-bundle.sh "$(BIN_DIR)/server-python"
	@echo ">> Ready: $(BIN_DIR)/server-python (run: cd $(BIN_DIR)/server-python && ./citadel.exe serve)"

package-windows-python: ## Stage + zip the Python-enabled Windows server ($(DIST_DIR)/citadel-windows-x86_64-python-v{version}.zip)
	@echo ">> Packaging Python-enabled Citadel v$(VERSION) for windows-x86_64"
	@source scripts/python-runtime-env.sh; export CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2; cargo build --release --features runtime-python
	$(eval PKG_NAME := citadel-windows-x86_64-python-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/scripts"
	@mkdir -p "$(PKG_STAGE)/maps"
	cp target/release/citadel.exe "$(PKG_STAGE)/citadel.exe"
	sed -e 's|# language = "lua"|language = "python"|' \
		-e 's|scripts_dir = "./game"|scripts_dir = "./scripts"|' \
		citadel.toml > "$(PKG_STAGE)/citadel.toml"
	cp packaging/server/scripts/main.py "$(PKG_STAGE)/scripts/main.py"
	cp packaging/server/README-python.txt "$(PKG_STAGE)/README.txt"
	bash scripts/stage-python-bundle.sh "$(PKG_STAGE)"
	bash scripts/smoke-python-bundle.sh "$(PKG_STAGE)"
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

# --- Ready-to-use engine client packages ----------------------------------
# Build + stage + zip each copy-into-project engine SDK as its own versioned
# Windows release archive (citadel-client-<engine>-windows-x86_64-v{version}.zip).
# These mirror the make.ps1 package-client-* targets the release CI reuses, so
# a published Release carries the server zip plus one zip per engine client.

package-client-unity: ## Stage + zip the Unity client SDK ($(DIST_DIR)/citadel-client-unity-windows-x86_64-v{version}.zip)
	@echo ">> Packaging Citadel unity client v$(VERSION) for windows-x86_64"
	cargo build --release -p citadel-client-ffi
	$(eval PKG_NAME := citadel-client-unity-windows-x86_64-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/Citadel" "$(PKG_STAGE)/Demo" "$(PKG_STAGE)/Plugins/x86_64"
	cp clients/unity/Citadel/*.cs "$(PKG_STAGE)/Citadel/"
	cp clients/unity/Demo/*.cs "$(PKG_STAGE)/Demo/"
	cp clients/unity/README.md "$(PKG_STAGE)/README.md"
	@if [ -f target/release/citadel_client_ffi.dll ]; then \
		cp target/release/citadel_client_ffi.dll "$(PKG_STAGE)/Plugins/x86_64/"; \
	elif [ -f target/release/libcitadel_client_ffi.dylib ]; then \
		cp target/release/libcitadel_client_ffi.dylib "$(PKG_STAGE)/Plugins/x86_64/"; \
	elif [ -f target/release/libcitadel_client_ffi.so ]; then \
		cp target/release/libcitadel_client_ffi.so "$(PKG_STAGE)/Plugins/x86_64/"; \
	else \
		echo "!! No cdylib found under target/release/"; exit 1; \
	fi
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

package-client-unreal: ## Stage + zip the Unreal client plugin ($(DIST_DIR)/citadel-client-unreal-windows-x86_64-v{version}.zip)
	@echo ">> Packaging Citadel unreal client v$(VERSION) for windows-x86_64"
	$(eval PKG_NAME := citadel-client-unreal-windows-x86_64-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/Plugins"
	cp -r clients/unreal/Plugin/Citadel "$(PKG_STAGE)/Plugins/Citadel"
	@rm -rf "$(PKG_STAGE)/Plugins/Citadel/Intermediate" \
		"$(PKG_STAGE)/Plugins/Citadel/Binaries" \
		"$(PKG_STAGE)/Plugins/Citadel/.uebuild" \
		"$(PKG_STAGE)/Plugins/Citadel/Source/CitadelClient/ThirdParty"
	cargo build --release -p citadel-client-ffi
	$(eval UE_TP := $(PKG_STAGE)/Plugins/Citadel/Source/CitadelClient/ThirdParty)
	@mkdir -p "$(UE_TP)/include"
	@case "$$(uname -s)" in \
		MINGW*|MSYS*|CYGWIN*|Windows_NT) plat=Win64; lib=citadel_client_ffi.lib ;; \
		Darwin) plat=Mac; lib=libcitadel_client_ffi.a ;; \
		Linux) plat=Linux; lib=libcitadel_client_ffi.a ;; \
		*) echo "!! package-client-unreal: unsupported host $$(uname -s)"; exit 1 ;; \
	esac; \
	mkdir -p "$(UE_TP)/$$plat"; \
	if [ "$$plat" = "Win64" ]; then dest=citadel_client_ffi.lib; else dest=libcitadel_client_ffi.a; fi; \
	cp "target/release/$$lib" "$(UE_TP)/$$plat/$$dest"; \
	cp crates/citadel-client-ffi/include/citadel_client.h "$(UE_TP)/include/citadel_client.h"
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

# The Godot SDK's GDScript delegates to a native GDExtension (CitadelClientNative)
# built from clients/godot/native/ over citadel-client-ffi, so the drop-in package
# ships the compiled Windows libraries, not just .gd source. This mirrors the
# make.ps1 Build-GodotDropInStage definition the release CI runs; it is a
# Windows-host build (needs MSVC), like package-windows. PYTHON defaults to
# `python`; SCons and a pinned godot-cpp checkout are provisioned on demand.
PYTHON ?= python
GODOT_BIN ?= godot
package-client-godot: ## Stage + zip the drop-in Godot client SDK with the prebuilt Windows GDExtension
	@echo ">> Packaging Citadel godot client v$(VERSION) for windows-x86_64"
	$(eval PKG_NAME := citadel-client-godot-windows-x86_64-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	$(eval GODOT_CPP := $(CURDIR)/target/godot-cpp)
	@$(PYTHON) -c "import SCons" 2>/dev/null || $(PYTHON) -m pip install --upgrade scons
	@if [ ! -f "$(GODOT_CPP)/SConstruct" ]; then \
		git clone --depth 1 --branch 4.3 https://github.com/godotengine/godot-cpp.git "$(GODOT_CPP)"; \
	fi
	cargo build --release -p citadel-client-ffi
	@rm -rf clients/godot/native/bin
	cd clients/godot/native && GODOT_CPP_PATH="$(GODOT_CPP)" CITADEL_FFI_LIB_DIR="$(CURDIR)/target/release" \
		$(PYTHON) -m SCons target=template_debug platform=windows arch=x86_64 build_profile=build_profile.json use_static_cpp=no
	cd clients/godot/native && GODOT_CPP_PATH="$(GODOT_CPP)" CITADEL_FFI_LIB_DIR="$(CURDIR)/target/release" \
		$(PYTHON) -m SCons target=template_release platform=windows arch=x86_64 build_profile=build_profile.json use_static_cpp=no
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/addons/citadel/bin"
	cp clients/godot/citadel/*.gd "$(PKG_STAGE)/addons/citadel/"
	cp clients/godot/native/citadel.gdextension "$(PKG_STAGE)/addons/citadel/"
	cp clients/godot/native/bin/*.dll "$(PKG_STAGE)/addons/citadel/bin/"
	cp -r clients/godot/sample "$(PKG_STAGE)/sample"
	cp clients/godot/README.md "$(PKG_STAGE)/README.md"
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

# The browser client remains a portable GDScript addon, but its official package
# also contains a real Godot Web export (`index.html`, `.js`, `.pck`, `.wasm`).
# It must stay separate from the native package: it does not load a GDExtension.
package-client-godot-web: ## Stage + zip the distributable Godot Web/WebAssembly SDK (requires GODOT_BIN)
	$(PYTHON) scripts/package_godot_web_artifact.py --godot "$(GODOT_BIN)"

package-clients-windows: package-client-unity package-client-unreal package-client-godot ## Stage + zip the Windows Unity, Unreal, and native Godot SDKs
	@echo ">> Packaged the Unity, Unreal, and Godot Windows client zips under $(DIST_DIR)/"

# --- macOS engine client packages -----------------------------------------
# These targets intentionally build only the active native architecture. The
# release workflow combines one Apple Silicon job and one Intel job, producing
# an installable package per engine and architecture without pretending that a
# extension cross-compiled on a different macOS SDK was tested.
package-client-unity-macos: ## Stage + zip the Unity macOS SDK for the native arch
	@case "$(MACOS_ARCH)" in aarch64|x86_64) ;; *) echo "!! Unsupported macOS architecture: $(MACOS_ARCH)"; exit 1 ;; esac
	@echo ">> Packaging Citadel unity client v$(VERSION) for macos-$(MACOS_ARCH)"
	MACOSX_DEPLOYMENT_TARGET="$(MACOS_DEPLOYMENT_TARGET)" cargo build --release -p citadel-client-ffi
	$(eval PKG_NAME := citadel-client-unity-macos-$(MACOS_ARCH)-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/Citadel" "$(PKG_STAGE)/Demo" "$(PKG_STAGE)/Plugins/macOS"
	cp clients/unity/Citadel/*.cs "$(PKG_STAGE)/Citadel/"
	cp clients/unity/Demo/*.cs "$(PKG_STAGE)/Demo/"
	cp clients/unity/README.md "$(PKG_STAGE)/README.md"
	cp target/release/libcitadel_client_ffi.dylib "$(PKG_STAGE)/Plugins/macOS/"
	@if [ -n "$(MACOS_SIGN_IDENTITY)" ]; then bash scripts/sign-macos-artifacts.sh "$(PKG_STAGE)" "$(MACOS_SIGN_IDENTITY)"; fi
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@if [ -n "$(MACOS_NOTARY_PROFILE)" ]; then \
		if [ -z "$(MACOS_SIGN_IDENTITY)" ]; then echo "!! MACOS_NOTARY_PROFILE requires MACOS_SIGN_IDENTITY"; exit 1; fi; \
		bash scripts/notarize-macos-archive.sh "$(DIST_DIR)/$(PKG_NAME).zip" "$(MACOS_NOTARY_PROFILE)"; \
	fi
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

package-client-unreal-macos: ## Stage + zip the Unreal macOS plugin for the native arch
	@case "$(MACOS_ARCH)" in aarch64|x86_64) ;; *) echo "!! Unsupported macOS architecture: $(MACOS_ARCH)"; exit 1 ;; esac
	@echo ">> Packaging Citadel unreal client v$(VERSION) for macos-$(MACOS_ARCH)"
	$(eval PKG_NAME := citadel-client-unreal-macos-$(MACOS_ARCH)-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/Plugins"
	cp -r clients/unreal/Plugin/Citadel "$(PKG_STAGE)/Plugins/Citadel"
	@rm -rf "$(PKG_STAGE)/Plugins/Citadel/Intermediate" \
		"$(PKG_STAGE)/Plugins/Citadel/Binaries" \
		"$(PKG_STAGE)/Plugins/Citadel/.uebuild" \
		"$(PKG_STAGE)/Plugins/Citadel/Source/CitadelClient/ThirdParty"
	MACOSX_DEPLOYMENT_TARGET="$(MACOS_DEPLOYMENT_TARGET)" cargo build --release -p citadel-client-ffi
	$(eval UE_TP := $(PKG_STAGE)/Plugins/Citadel/Source/CitadelClient/ThirdParty)
	@mkdir -p "$(UE_TP)/Mac" "$(UE_TP)/include"
	cp target/release/libcitadel_client_ffi.a "$(UE_TP)/Mac/libcitadel_client_ffi.a"
	cp crates/citadel-client-ffi/include/citadel_client.h "$(UE_TP)/include/citadel_client.h"
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

package-client-godot-macos: ## Stage + zip the Godot macOS SDK for the native arch
	@case "$(MACOS_ARCH)" in aarch64|x86_64) ;; *) echo "!! Unsupported macOS architecture: $(MACOS_ARCH)"; exit 1 ;; esac
	@echo ">> Packaging Citadel godot client v$(VERSION) for macos-$(MACOS_ARCH)"
	$(eval PKG_NAME := citadel-client-godot-macos-$(MACOS_ARCH)-v$(VERSION))
	$(eval PKG_STAGE := $(DIST_DIR)/$(PKG_NAME))
	$(eval GODOT_CPP := $(CURDIR)/target/godot-cpp)
	@$(MACOS_PYTHON) -m venv "$(MACOS_GODOT_VENV)"
	@$(MACOS_GODOT_PYTHON) -m pip install --quiet --upgrade pip scons
	@if [ ! -f "$(GODOT_CPP)/SConstruct" ]; then \
		git clone --depth 1 --branch 4.3 https://github.com/godotengine/godot-cpp.git "$(GODOT_CPP)"; \
	fi
	MACOSX_DEPLOYMENT_TARGET="$(MACOS_DEPLOYMENT_TARGET)" cargo build --release -p citadel-client-ffi
	@rm -rf clients/godot/native/bin
	cd clients/godot/native && GODOT_CPP_PATH="$(GODOT_CPP)" CITADEL_FFI_LIB_DIR="$(CURDIR)/target/release" \
		MACOSX_DEPLOYMENT_TARGET="$(MACOS_DEPLOYMENT_TARGET)" $(MACOS_GODOT_PYTHON) -m SCons target=template_debug platform=macos arch=$(MACOS_GODOT_ARCH) build_profile=build_profile.json use_static_cpp=no
	cd clients/godot/native && GODOT_CPP_PATH="$(GODOT_CPP)" CITADEL_FFI_LIB_DIR="$(CURDIR)/target/release" \
		MACOSX_DEPLOYMENT_TARGET="$(MACOS_DEPLOYMENT_TARGET)" $(MACOS_GODOT_PYTHON) -m SCons target=template_release platform=macos arch=$(MACOS_GODOT_ARCH) build_profile=build_profile.json use_static_cpp=no
	@rm -rf "$(PKG_STAGE)"
	@mkdir -p "$(PKG_STAGE)/addons/citadel/bin"
	cp clients/godot/citadel/*.gd "$(PKG_STAGE)/addons/citadel/"
	cp clients/godot/native/citadel.gdextension "$(PKG_STAGE)/addons/citadel/"
	cp clients/godot/native/bin/*.dylib "$(PKG_STAGE)/addons/citadel/bin/"
	cp -r clients/godot/sample "$(PKG_STAGE)/sample"
	cp clients/godot/README.md "$(PKG_STAGE)/README.md"
	@if [ -n "$(MACOS_SIGN_IDENTITY)" ]; then bash scripts/sign-macos-artifacts.sh "$(PKG_STAGE)" "$(MACOS_SIGN_IDENTITY)"; fi
	@rm -f "$(DIST_DIR)/$(PKG_NAME).zip"
	cd "$(DIST_DIR)" && zip -r "$(PKG_NAME).zip" "$(PKG_NAME)"
	@if [ -n "$(MACOS_NOTARY_PROFILE)" ]; then \
		if [ -z "$(MACOS_SIGN_IDENTITY)" ]; then echo "!! MACOS_NOTARY_PROFILE requires MACOS_SIGN_IDENTITY"; exit 1; fi; \
		bash scripts/notarize-macos-archive.sh "$(DIST_DIR)/$(PKG_NAME).zip" "$(MACOS_NOTARY_PROFILE)"; \
	fi
	@echo ">> Packaged $(DIST_DIR)/$(PKG_NAME).zip"

package-clients-macos: package-client-unity-macos package-client-unreal-macos package-client-godot-macos ## Stage + zip macOS Unity, Unreal, and Godot SDKs for the native arch
	@echo ">> Packaged macOS Unity, Unreal, and Godot client zips under $(DIST_DIR)/"

bin-benchmark: ## Stage the combat benchmark at $(BENCHMARK_DIR) (server.exe + Lua + HTML client + JS SDK)
	@echo ">> Staging combat benchmark at $(BENCHMARK_DIR)"
	cargo build --release
	@mkdir -p "$(BENCHMARK_DIR)/scripts" "$(BENCHMARK_DIR)/clients/js"
	@rm -rf "$(BENCHMARK_DIR)/clients/js/src"
	@mkdir -p "$(BENCHMARK_DIR)/clients/js/src"
	@if [ -f target/release/citadel.exe ]; then \
		cp target/release/citadel.exe "$(BENCHMARK_DIR)/server.exe"; \
	else \
		cp target/release/citadel "$(BENCHMARK_DIR)/server.exe"; \
	fi
	sed -e 's|scripts_dir = "./game"|scripts_dir = "./scripts"|' \
		-e 's|tick_hz = 0|tick_hz = 20|' \
		citadel.toml > "$(BENCHMARK_DIR)/citadel.toml"
	cp crates/citadel-client/examples/combat_server.lua "$(BENCHMARK_DIR)/scripts/main.lua"
	sed 's|../../../clients/js/src/index.js|./clients/js/src/index.js|' \
		crates/citadel-client/examples/combat_viz.html > "$(BENCHMARK_DIR)/client.html"
	cp clients/js/src/*.js "$(BENCHMARK_DIR)/clients/js/src/"
	@printf '%s\n' \
		'Citadel combat benchmark' \
		'' \
		'1. Start the server from this folder:' \
		'   ./server.exe serve' \
		'' \
		'2. From the repository root, serve this folder:' \
		'   python3 -m http.server $(BENCHMARK_WEB_PORT) --directory bin/benchmark' \
		'' \
		'3. Open:' \
		'   http://127.0.0.1:$(BENCHMARK_WEB_PORT)/client.html' \
		'' \
		'The HTML defaults to 30 bots and connects to ws://127.0.0.1:7352/.' \
		'Run `make bin-benchmark` again after source changes to refresh this folder.' \
		> "$(BENCHMARK_DIR)/README.txt"
	@echo ">> Ready: $(BENCHMARK_DIR)"
	@echo ">> Server: cd $(BENCHMARK_DIR) && ./server.exe serve"
	@echo ">> Client: python3 -m http.server $(BENCHMARK_WEB_PORT) --directory $(BENCHMARK_DIR)"
	@echo ">> Open: http://127.0.0.1:$(BENCHMARK_WEB_PORT)/client.html"

benchmark-serve: bin-benchmark ## Stage, run server, serve HTML, and open the benchmark client
	@URL="http://127.0.0.1:$(BENCHMARK_WEB_PORT)/client.html"; \
	echo ">> Starting benchmark server from $(BENCHMARK_DIR)"; \
	(cd "$(BENCHMARK_DIR)" && ./server.exe serve) & SRV=$$!; \
	echo ">> Serving benchmark client at $$URL"; \
	python3 -m http.server $(BENCHMARK_WEB_PORT) --directory "$(BENCHMARK_DIR)" & WEB=$$!; \
	cleanup() { kill $$SRV $$WEB 2>/dev/null || true; }; \
	trap cleanup EXIT INT TERM; \
	sleep $(WAIT); \
	if ! kill -0 $$SRV 2>/dev/null; then echo "!! benchmark server exited early"; exit 1; fi; \
	if ! kill -0 $$WEB 2>/dev/null; then echo "!! benchmark web server exited early; is port $(BENCHMARK_WEB_PORT) busy?"; exit 1; fi; \
	if command -v open >/dev/null 2>&1; then open "$$URL"; \
	elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$$URL" >/dev/null 2>&1 || true; \
	else echo ">> Open $$URL"; fi; \
	echo ">> Benchmark running. Press Ctrl-C to stop server + HTTP."; \
	wait $$SRV $$WEB

# --- Client SDK staging (bin/clients/<engine>) -----------------------------
# Copy-into-project SDK source per docs/architecture/client-sdk-layout.md:
# ship the SDK SOURCE (the engine compiles/interprets it) plus the built
# native FFI cdylib only where that SDK actually loads one (Unity, Unreal).
# Godot release packages compile a GDExtension over the FFI; its lightweight
# local staging target remains source-only. Re-running a target wipes and
# re-stages its folder.

bin-client-unity: ## Stage the Unity SDK (bindings + demo + built FFI) at $(CLIENTS_DIR)/unity
	@echo ">> Staging Unity SDK at $(CLIENTS_DIR)/unity"
	cargo build --release -p citadel-client-ffi
	@rm -rf "$(CLIENTS_DIR)/unity"
	@mkdir -p "$(CLIENTS_DIR)/unity/Citadel" "$(CLIENTS_DIR)/unity/Demo" \
		"$(CLIENTS_DIR)/unity/Plugins/x86_64" "$(CLIENTS_DIR)/unity/Plugins/macOS"
	cp clients/unity/Citadel/*.cs "$(CLIENTS_DIR)/unity/Citadel/"
	cp clients/unity/Demo/*.cs "$(CLIENTS_DIR)/unity/Demo/"
	cp clients/unity/README.md "$(CLIENTS_DIR)/unity/README.md"
	@if [ -f target/release/citadel_client_ffi.dll ]; then \
		cp target/release/citadel_client_ffi.dll "$(CLIENTS_DIR)/unity/Plugins/x86_64/"; \
		echo ">> Installed citadel_client_ffi.dll -> $(CLIENTS_DIR)/unity/Plugins/x86_64/"; \
	elif [ -f target/release/libcitadel_client_ffi.dylib ]; then \
		cp target/release/libcitadel_client_ffi.dylib "$(CLIENTS_DIR)/unity/Plugins/macOS/"; \
		echo ">> Installed libcitadel_client_ffi.dylib -> $(CLIENTS_DIR)/unity/Plugins/macOS/"; \
	elif [ -f target/release/libcitadel_client_ffi.so ]; then \
		cp target/release/libcitadel_client_ffi.so "$(CLIENTS_DIR)/unity/Plugins/x86_64/"; \
		echo ">> Installed libcitadel_client_ffi.so -> $(CLIENTS_DIR)/unity/Plugins/x86_64/"; \
	else \
		echo "!! No cdylib found under target/release/"; exit 1; \
	fi
	@echo ">> Ready: $(CLIENTS_DIR)/unity (copy Citadel/ and Demo/ into your project's Assets/)"

bin-client-unreal: ## Stage the Unreal plugin (drop-in source + built FFI) at $(CLIENTS_DIR)/unreal/Plugins/Citadel
	@echo ">> Staging Unreal SDK at $(CLIENTS_DIR)/unreal"
	@rm -rf "$(CLIENTS_DIR)/unreal"
	@mkdir -p "$(CLIENTS_DIR)/unreal/Plugins"
	cp -r clients/unreal/Plugin/Citadel "$(CLIENTS_DIR)/unreal/Plugins/Citadel"
	@rm -rf "$(CLIENTS_DIR)/unreal/Plugins/Citadel/Intermediate" \
		"$(CLIENTS_DIR)/unreal/Plugins/Citadel/Binaries" \
		"$(CLIENTS_DIR)/unreal/Plugins/Citadel/.uebuild" \
		"$(CLIENTS_DIR)/unreal/Plugins/Citadel/Source/CitadelClient/ThirdParty"
	cargo build --release -p citadel-client-ffi
	$(eval UE_TP := $(CLIENTS_DIR)/unreal/Plugins/Citadel/Source/CitadelClient/ThirdParty)
	@mkdir -p "$(UE_TP)/include"
	@case "$$(uname -s)" in \
		MINGW*|MSYS*|CYGWIN*|Windows_NT) plat=Win64; lib=citadel_client_ffi.lib ;; \
		Darwin) plat=Mac; lib=libcitadel_client_ffi.a ;; \
		Linux) plat=Linux; lib=libcitadel_client_ffi.a ;; \
		*) echo "!! bin-client-unreal: unsupported host $$(uname -s)"; exit 1 ;; \
	esac; \
	mkdir -p "$(UE_TP)/$$plat"; \
	if [ "$$plat" = "Win64" ]; then dest=citadel_client_ffi.lib; else dest=libcitadel_client_ffi.a; fi; \
	cp "target/release/$$lib" "$(UE_TP)/$$plat/$$dest"; \
	cp crates/citadel-client-ffi/include/citadel_client.h "$(UE_TP)/include/citadel_client.h"
	@echo ">> Ready: $(CLIENTS_DIR)/unreal/Plugins/Citadel (drop into <YourProject>/Plugins/Citadel)"

bin-client-godot: ## Stage the Godot addon source at $(CLIENTS_DIR)/godot
	@echo ">> Staging Godot SDK at $(CLIENTS_DIR)/godot"
	@rm -rf "$(CLIENTS_DIR)/godot"
	@mkdir -p "$(CLIENTS_DIR)/godot"
	cp -r clients/godot/citadel "$(CLIENTS_DIR)/godot/citadel"
	cp -r clients/godot/sample "$(CLIENTS_DIR)/godot/sample"
	cp clients/godot/README.md "$(CLIENTS_DIR)/godot/README.md"
	@echo ">> Ready: $(CLIENTS_DIR)/godot (copy citadel/ into res://addons/citadel/; use package-client-godot* for a native GDExtension)"

bin-client-godot-web: ## Stage the reusable GDScript Godot Web addon at $(CLIENTS_DIR)/godot-web
	@echo ">> Staging Godot Web SDK at $(CLIENTS_DIR)/godot-web"
	@rm -rf "$(CLIENTS_DIR)/godot-web"
	@mkdir -p "$(CLIENTS_DIR)/godot-web/addons"
	cp -r clients/godot/citadel "$(CLIENTS_DIR)/godot-web/addons/citadel"
	cp clients/godot/README.md "$(CLIENTS_DIR)/godot-web/README.md"
	cp clients/godot/sdk.manifest.json "$(CLIENTS_DIR)/godot-web/sdk.manifest.json"
	@echo ">> Ready: $(CLIENTS_DIR)/godot-web (copy addons/ into your Web project's res:// root; no GDExtension is included)"

bin-client-js: ## Stage the JS/Web SDK (src + Three.js starter + types + package.json, no tests) at $(CLIENTS_DIR)/js
	@echo ">> Staging JS SDK at $(CLIENTS_DIR)/js"
	@rm -rf "$(CLIENTS_DIR)/js"
	@mkdir -p "$(CLIENTS_DIR)/js"
	cp -r clients/js/src "$(CLIENTS_DIR)/js/src"
	cp -r clients/js/examples "$(CLIENTS_DIR)/js/examples"
	cp clients/js/index.d.ts "$(CLIENTS_DIR)/js/index.d.ts"
	cp clients/js/package.json "$(CLIENTS_DIR)/js/package.json"
	cp clients/js/README.md "$(CLIENTS_DIR)/js/README.md"
	@echo ">> Ready: $(CLIENTS_DIR)/js (includes examples/threejs-starter; npm install @citadel/client once published)"

bin-client-rust: ## Stage the Rust client crate source at $(CLIENTS_DIR)/rust/citadel-client
	@echo ">> Staging Rust client SDK at $(CLIENTS_DIR)/rust/citadel-client"
	@rm -rf "$(CLIENTS_DIR)/rust"
	@mkdir -p "$(CLIENTS_DIR)/rust/citadel-client"
	cp crates/citadel-client/Cargo.toml "$(CLIENTS_DIR)/rust/citadel-client/Cargo.toml"
	cp -r crates/citadel-client/src "$(CLIENTS_DIR)/rust/citadel-client/src"
	cp -r crates/citadel-client/examples "$(CLIENTS_DIR)/rust/citadel-client/examples"
	@printf '%s\n' \
		'Citadel Rust client SDK (crates/citadel-client)' \
		'' \
		'This is staged SOURCE, consumed as a path or git Cargo dependency --' \
		'not a standalone crate published to crates.io. Its Cargo.toml still' \
		'references sibling workspace crates (e.g. citadel-wire) by relative' \
		'path, so build it from within a checkout of the Citadel repo, or vendor' \
		'those sibling crates too. Point your own Cargo.toml at this folder' \
		'(path dependency) or at the citadel repo (git dependency).' \
		> "$(CLIENTS_DIR)/rust/citadel-client/README.txt"
	@echo ">> Ready: $(CLIENTS_DIR)/rust/citadel-client"

bin-clients: bin-client-unity bin-client-unreal bin-client-godot bin-client-godot-web bin-client-js bin-client-rust ## Stage all client SDKs under $(CLIENTS_DIR)/
	@echo ">> All client SDKs staged under $(CLIENTS_DIR)/"

bin-all: bin-server bin-benchmark bin-clients ## Stage everything (server, benchmark, all client SDKs) under $(BIN_DIR)/
	@echo ">> Everything staged under $(BIN_DIR)/"

# --- Local Postgres (persistence layer; ) ------------------------

db-up: ## Start a throwaway Postgres in Docker and apply migrations
	@docker rm -f $(PG_CONTAINER) >/dev/null 2>&1 || true
	docker run -d --name $(PG_CONTAINER) \
		-e POSTGRES_USER=$(PG_USER) \
		-e POSTGRES_PASSWORD=$(PG_PASSWORD) \
		-e POSTGRES_DB=$(PG_DB) \
		-p $(PG_PORT):5432 $(PG_IMAGE)
	@echo ">> Waiting for Postgres to accept connections..."
	@for i in $$(seq 1 40); do \
		if docker exec $(PG_CONTAINER) pg_isready -U $(PG_USER) -d $(PG_DB) >/dev/null 2>&1; then \
			echo ">> Postgres ready on localhost:$(PG_PORT)"; break; \
		fi; sleep 1; \
	done
	@$(MAKE) db-migrate
	@echo ">> DATABASE_URL=$(DATABASE_URL)"

db-down: ## Stop and remove the throwaway Postgres container
	docker rm -f $(PG_CONTAINER)

db-migrate: ## Apply migrations to DATABASE_URL (default: the local container)
	DATABASE_URL=$(DATABASE_URL) cargo run --example db_migrate

# --- Public documentation site (local-only; outside the Cargo workspace) ---

docs-install: ## Install the docs site's Node dependencies (website/)
	cd $(DOCS_DIR) && (npm ci || npm install)

docs-build: ## Build the docs site + generate rustdoc into website/public/rustdoc/
	cargo doc --no-deps --workspace
	@rm -rf $(DOCS_DIR)/public/rustdoc
	@mkdir -p $(DOCS_DIR)/public/rustdoc
	@cp -R target/doc/. $(DOCS_DIR)/public/rustdoc/
	cd $(DOCS_DIR) && npm run build

docs-serve: ## Preview the built docs site locally (build first if needed)
	cd $(DOCS_DIR) && npm run preview
