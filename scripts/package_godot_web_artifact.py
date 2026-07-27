#!/usr/bin/env python3
"""Build and validate the distributable Godot Web SDK archive.

The archive intentionally contains two things under one versioned root:
the reusable ``addons/citadel`` GDScript SDK and a small exported Godot Web
verification project. The latter proves that the browser surface is a real
HTML/JavaScript/PCK/WebAssembly delivery, not merely an addon source ZIP.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


REQUIRED_ADDON_FILES = (
    "addons/citadel/protocol.gd",
    "addons/citadel/client.gd",
    "addons/citadel/web_client.gd",
    "addons/citadel/rooms.gd",
)
REQUIRED_WEB_FILES = (
    "web/index.html",
    "web/index.js",
    "web/index.pck",
    "web/index.wasm",
    "web/citadel-e2e.toml",
    "web/serve_web.py",
)
NATIVE_SUFFIXES = (".gdextension", ".dll", ".dylib", ".so")


def fail(message: str) -> None:
    raise RuntimeError(message)


def cargo_version(cargo_toml: Path) -> str:
    match = re.search(
        r'^version\s*=\s*"([^"]+)"\s*$', cargo_toml.read_text(encoding="utf-8"), re.MULTILINE
    )
    if match is None:
        fail(f"could not read package version from {cargo_toml}")
    return match.group(1)


def package_root_from_names(names: set[str]) -> str:
    roots = {name.split("/", 1)[0] for name in names if "/" in name}
    matching = [root for root in roots if root.startswith("citadel-client-godot-web-v")]
    if len(matching) != 1:
        fail("archive must contain exactly one citadel-client-godot-web-v<version> root")
    return matching[0]


def validate_archive(archive: Path) -> None:
    if not archive.is_file():
        fail(f"archive does not exist: {archive}")

    with zipfile.ZipFile(archive) as zip_file:
        names = {name for name in zip_file.namelist() if not name.endswith("/")}
        root = package_root_from_names(names)
        required = tuple(f"{root}/{path}" for path in (*REQUIRED_ADDON_FILES, *REQUIRED_WEB_FILES))
        missing = [path for path in required if path not in names]
        if missing:
            fail("archive is missing required files: " + ", ".join(missing))

        native = [name for name in names if name.endswith(NATIVE_SUFFIXES)]
        if native:
            fail("browser archive contains native artifacts: " + ", ".join(native))
        generated_state = [name for name in names if "/.godot/" in name]
        if generated_state:
            fail("archive contains generated Godot state: " + ", ".join(generated_state))

        wasm = zip_file.read(f"{root}/web/index.wasm")
        if wasm[:4] != b"\x00asm":
            fail("web/index.wasm does not start with the WebAssembly magic bytes")

        html = zip_file.read(f"{root}/web/index.html").decode("utf-8", errors="replace")
        javascript = zip_file.read(f"{root}/web/index.js").decode("utf-8", errors="replace")
        if "index.js" not in html:
            fail("web/index.html does not reference its JavaScript bootstrap")
        if "wasm" not in javascript or "pck" not in javascript:
            fail("web/index.js does not reference both the WebAssembly and PCK payloads")


def copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def stage_export_project(repo_root: Path, stage: Path) -> Path:
    source = repo_root / "clients" / "godot"
    fixture = source / "tests" / "web"
    web_root = stage / "web"
    addon_root = stage / "addons" / "citadel"

    shutil.copytree(source / "citadel", addon_root)
    shutil.copytree(addon_root, web_root / "addons" / "citadel")
    copy_file(source / "README.md", stage / "README.md")
    copy_file(source / "sdk.manifest.json", stage / "sdk.manifest.json")
    for name in (
        "project.godot",
        "export_presets.cfg",
        "smoke.gd",
        "smoke.tscn",
        "citadel-e2e.toml",
        "serve_web.py",
        "README.md",
    ):
        copy_file(fixture / name, web_root / name)
    return web_root


def write_zip(stage: Path, archive: Path) -> None:
    if archive.exists():
        archive.unlink()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
        for path in sorted(stage.rglob("*")):
            if not path.is_file() or ".godot" in path.parts:
                continue
            zip_file.write(path, path.relative_to(stage.parent).as_posix())


def build_archive(repo_root: Path, dist_dir: Path, godot: str) -> Path:
    version = cargo_version(repo_root / "Cargo.toml")
    package_name = f"citadel-client-godot-web-v{version}"
    stage = dist_dir / package_name
    archive = dist_dir / f"{package_name}.zip"
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)

    web_root = stage_export_project(repo_root, stage)
    command = [godot, "--headless", "--path", str(web_root), "--export-release", "Web", "index.html"]
    print(">> " + " ".join(command), flush=True)
    try:
        subprocess.run(command, check=True)
    except FileNotFoundError:
        fail(f"Godot executable was not found: {godot}. Set GODOT_BIN or pass --godot.")
    finally:
        generated_state = web_root / ".godot"
        if generated_state.exists():
            shutil.rmtree(generated_state)

    write_zip(stage, archive)
    validate_archive(archive)
    print(f">> Packaged {archive}")
    return archive


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--godot", default="godot", help="Godot 4 executable with Web export templates")
    parser.add_argument("--dist-dir", type=Path, help="artifact directory (defaults to <repo>/dist)")
    parser.add_argument("--verify-package", type=Path, help="validate an existing Web SDK ZIP and exit")
    args = parser.parse_args()

    try:
        if args.verify_package is not None:
            validate_archive(args.verify_package)
            print(f"godot-web-artifact: OK ({args.verify_package})")
            return 0

        repo_root = Path(__file__).resolve().parents[1]
        dist_dir = args.dist_dir if args.dist_dir is not None else repo_root / "dist"
        build_archive(repo_root, dist_dir.resolve(), args.godot)
        return 0
    except (OSError, RuntimeError, subprocess.CalledProcessError, zipfile.BadZipFile) as error:
        print(f"godot-web-artifact: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
