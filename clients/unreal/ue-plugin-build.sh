#!/usr/bin/env bash
#
# Citadel Unreal SDK — gated UE 5.8 plugin compile-verification.
#
# This is the "real" Tier-B for the Unreal SDK: it compiles the plugin's UE C++
# (UObject reflection, subsystems, components, the wire/quantizer ports) against
# REAL Unreal Engine headers + the canonical Citadel C ABI header — the drift the
# UE-free object-only TU in parity-hook.sh cannot catch.
#
# It is OPT-IN and NOT part of the default `bash scripts/check.sh` (a UE build is
# far too slow for the fast path). Run it directly, or drive it from
# parity-hook.sh by setting CITADEL_UE_BUILD=1.
#
# UE root resolution:
#   * $CITADEL_UE_ROOT if set;
#   * else D:/Games/UE_5.8 if it exists;
#   * else SKIP cleanly (exit 0) — a machine without UE must not fail.
#
# Native lib: to verify a clean COMPILE + LINK without the real citadel-client-ffi
# library present, the plugin module compiles an in-tree C ABI stub
# (CITADEL_FFI_STUB=1; see CitadelClient.Build.cs / CitadelFfiStub.cpp). Real
# behavior (linking the built native lib) is a downstream/consumer step and an
# in-editor manual verification; both are out of scope for this compile gate.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

# Windows-form paths for UBT / the generated .uproject (UE wants D:/... paths).
# The drop-in plugin now lives at clients/unreal/Plugin/Citadel/ (= a standard
# <Project>/Plugins/Citadel/); Citadel.uplugin sits there.
plugin_dir_win="$(cd "$script_dir/Plugin/Citadel" && pwd -W 2>/dev/null || (cd "$script_dir/Plugin/Citadel" && pwd))"
# UBT's AdditionalPluginDirectories discovers .uplugin files in the *subfolders*
# of each listed directory (not the directory itself). Citadel.uplugin lives in
# clients/unreal/Plugin/Citadel, so we must point UBT at its PARENT,
# clients/unreal/Plugin, whose subfolder `Citadel` UBT then recognizes as the
# plugin.
plugins_dir_win="$(cd "$script_dir/Plugin" && pwd -W 2>/dev/null || (cd "$script_dir/Plugin" && pwd))"

# --- Resolve the UE root -------------------------------------------------------
ue_root="${CITADEL_UE_ROOT:-D:/Games/UE_5.8}"
if [[ ! -d "$ue_root" ]]; then
  echo "unreal UE-compile: SKIP — no Unreal Engine root found."
  echo "unreal UE-compile: set CITADEL_UE_ROOT=<UE install> (looked for '$ue_root')."
  exit 0
fi

build_bat="$ue_root/Engine/Build/BatchFiles/Build.bat"
if [[ ! -f "$build_bat" ]]; then
  echo "unreal UE-compile: SKIP — '$build_bat' not found under CITADEL_UE_ROOT='$ue_root'."
  exit 0
fi

echo "unreal UE-compile: using UE root '$ue_root'"

# --- Generate a minimal host project that force-compiles the plugin ------------
# UBT only discovers plugin modules under <PluginRoot>/Source/, and a plugin does
# not build on its own — a host project (or RunUAT BuildPlugin) drives it. We use
# a tiny C++ host project that depends on the CitadelClient module so building the
# host's editor target compiles + links the plugin against real UE headers. The
# plugin is referenced in-place via AdditionalPluginDirectories so its Build.cs
# ModuleDirectory stays in the repo and the C ABI include path resolves.
host_dir="${CITADEL_UE_BUILD_DIR:-$script_dir/.uebuild/CitadelHost}"
mkdir -p "$host_dir/Source/CitadelHost"
host_dir_win="$(cd "$host_dir" && pwd -W 2>/dev/null || pwd)"

cat > "$host_dir/CitadelHost.uproject" <<JSON
{
	"FileVersion": 3,
	"EngineAssociation": "",
	"Category": "",
	"Description": "Scratch host project to compile-verify the CitadelClient plugin.Generated; not committed.",
	"Modules": [
		{ "Name": "CitadelHost", "Type": "Runtime", "LoadingPhase": "Default" }
	],
	"Plugins": [
		{ "Name": "Citadel", "Enabled": true }
	],
	"AdditionalPluginDirectories": [
		"$plugins_dir_win"
	]
}
JSON

cat > "$host_dir/Source/CitadelHost.Target.cs" <<'CS'
using UnrealBuildTool;

public class CitadelHostTarget : TargetRules
{
	public CitadelHostTarget(TargetInfo Target) : base(Target)
	{
		Type = TargetType.Game;
		DefaultBuildSettings = BuildSettingsVersion.Latest;
		IncludeOrderVersion = EngineIncludeOrderVersion.Latest;
		ExtraModuleNames.Add("CitadelHost");
	}
}
CS

cat > "$host_dir/Source/CitadelHostEditor.Target.cs" <<'CS'
using UnrealBuildTool;

public class CitadelHostEditorTarget : TargetRules
{
	public CitadelHostEditorTarget(TargetInfo Target) : base(Target)
	{
		Type = TargetType.Editor;
		DefaultBuildSettings = BuildSettingsVersion.Latest;
		IncludeOrderVersion = EngineIncludeOrderVersion.Latest;
		ExtraModuleNames.Add("CitadelHost");
	}
}
CS

cat > "$host_dir/Source/CitadelHost/CitadelHost.Build.cs" <<'CS'
using UnrealBuildTool;

public class CitadelHost : ModuleRules
{
	public CitadelHost(ReadOnlyTargetRules Target) : base(Target)
	{
		PCHUsage = PCHUsageMode.UseExplicitOrSharedPCHs;
		PublicDependencyModuleNames.AddRange(new string[] { "Core", "CoreUObject", "Engine" });
		// Force the plugin module to compile + link even though the host has no
		// code that references it.
		PrivateDependencyModuleNames.Add("CitadelClient");
	}
}
CS

cat > "$host_dir/Source/CitadelHost/CitadelHost.h" <<'CPP'
#pragma once

#include "CoreMinimal.h"
CPP

cat > "$host_dir/Source/CitadelHost/CitadelHost.cpp" <<'CPP'
#include "CitadelHost.h"
#include "Modules/ModuleManager.h"

IMPLEMENT_PRIMARY_GAME_MODULE(FDefaultGameModuleImpl, CitadelHost, "CitadelHost");
CPP

# --- Build the editor target (compiles the plugin) -----------------------------
# CITADEL_FFI_STUB=1 => the plugin compiles the in-tree C ABI stub so the module
# links without the real native lib present.
export CITADEL_FFI_STUB=1
uproject_win="$host_dir_win/CitadelHost.uproject"

echo "unreal UE-compile: building CitadelHostEditor (Win64 Development)"
echo "unreal UE-compile:   project = $uproject_win"
echo "unreal UE-compile:   plugin  = $plugin_dir_win"

# Invoke Build.bat directly (Git Bash runs .bat via cmd). MSYS must not rewrite
# the D:/ paths or the -project= argument.
set +e
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" \
  "$build_bat" CitadelHostEditor Win64 Development "-project=$uproject_win" -waitmutex -NoHotReloadFromIDE
status=$?
set -e

if [[ $status -eq 0 ]]; then
  echo "unreal UE-compile: OK — CitadelClient plugin compiled + linked against UE at '$ue_root'."
else
  echo "unreal UE-compile: FAILED — see the UBT output above (exit $status)." >&2
fi
exit $status
