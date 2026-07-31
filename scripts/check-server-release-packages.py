#!/usr/bin/env python3
"""Keep server-release staging separate from client SDK release artifacts."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent


def rule(source: str, name: str) -> str:
    match = re.search(rf"^{re.escape(name)}:.*?(?=^[A-Za-z0-9_-]+:|\Z)", source, re.M | re.S)
    if not match:
        raise ValueError(f"missing Makefile target {name}")
    return match.group(0)


def function(source: str, name: str) -> str:
    match = re.search(rf"^function {re.escape(name)} \{{.*?(?=^function |\Z)", source, re.M | re.S)
    if not match:
        raise ValueError(f"missing PowerShell function {name}")
    return match.group(0)


def main() -> int:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    powershell = (ROOT / "make.ps1").read_text(encoding="utf-8")
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    windows_readme = (ROOT / "packaging/windows/README.md").read_text(encoding="utf-8")
    macos_readme = (ROOT / "packaging/macos/README.md").read_text(encoding="utf-8")

    errors: list[str] = []
    try:
        server_rules = {name: rule(makefile, name) for name in ("package-windows", "package-linux", "package-macos")}
        windows_function = function(powershell, "Invoke-PackageWindows")
    except ValueError as error:
        errors.append(str(error))
        server_rules, windows_function = {}, ""

    forbidden = ("clients/", "clients\\", "citadel-client-ffi", "unity-README")
    for name, source in server_rules.items():
        for text in forbidden:
            if text in source:
                errors.append(f"{name} stages or builds client content: {text}")
    for text in forbidden:
        if text in windows_function:
            errors.append(f"Invoke-PackageWindows stages or builds client content: {text}")

    required_workflow = ("./make.ps1 package-windows", "./make.ps1 package-clients-windows")
    for text in required_workflow:
        if text not in workflow:
            errors.append(f"release workflow is missing separate Windows package command: {text}")
    for readme_name, source in (("Windows", windows_readme), ("macOS", macos_readme)):
        if "Server archives intentionally contain no client SDKs." not in source:
            errors.append(f"{readme_name} server package README does not document the client boundary")

    if errors:
        print("check-server-release-packages: " + "; ".join(errors), file=sys.stderr)
        return 1
    print("check-server-release-packages: server package definitions are client-free")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
