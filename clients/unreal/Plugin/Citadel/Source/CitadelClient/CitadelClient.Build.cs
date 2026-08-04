// CitadelClient.Build.cs — UnrealBuildTool module rules for the Citadel Unreal
// C++ SDK. This turns clients/unreal/ into a real UE plugin module
// that compiles against real UE headers + the canonical Citadel C ABI header.
//
// The SDK is HEADER-DRIVEN: it includes
// the cbindgen-generated crates/citadel-client-ffi/include/citadel_client.h
// VERBATIM and never re-declares the C prototypes. CITADEL_WITH_UNREAL=1 tells
// CitadelWire.h to use Unreal's own sized-int aliases instead of the <cstdint>
// standalone fallback.
//
// Native library: the plugin calls into the citadel-client-ffi native lib. Per
// the SDK-source-only convention, that lib is built at package time and is never
// committed. Consumers point CITADEL_FFI_LIB at their built import/static lib
// (see clients/unreal/README.md). The gated compile-verification build
// (ue-plugin-build.sh) instead sets CITADEL_FFI_STUB=1 to compile a tiny in-tree
// stub of the C ABI so the module links cleanly without the real lib — this
// verifies the C++ compiles and links; it does not exercise real behavior.
using System;
using System.IO;
using UnrealBuildTool;

public class CitadelClient : ModuleRules
{
	public CitadelClient(ReadOnlyTargetRules Target) : base(Target)
	{
		PCHUsage = PCHUsageMode.UseExplicitOrSharedPCHs;

		PublicDependencyModuleNames.AddRange(new string[]
		{
			"Core",
			"CoreUObject",
			"Engine",
			// HTTP backs the Blueprint device/custom authenticate path
			// (POST /v1/auth/device|custom -> session token). It is a PUBLIC
			// dependency because CitadelClientSubsystem.h references FHttpRequestPtr
			// in its declared auth-callback signature, so any downstream module that
			// includes the subsystem header must see the HTTP module too.
			"HTTP",
		});

		// Json is used only inside the .cpp to build the request body and parse the
		// session-token response, so it stays a private dependency.
		PrivateDependencyModuleNames.AddRange(new string[]
		{
			"Json",
		});

		// Header-driven binding: use Unreal's uint8/uint16/uint32 aliases.
		PublicDefinitions.Add("CITADEL_WITH_UNREAL=1");

		// citadel_client.h resolution, drop-in first:
		//   (1) Bundled inside the plugin — <Module>/ThirdParty/include/. This is
		//       what makes a copied plugin self-contained. Populated by
		//       clients/unreal/bundle-ffi.sh (dev) or the release package (CI);
		//       gitignored so we never commit the generated header.
		//   (2) In-repo build — the canonical cbindgen header six levels up (a
		//       host project with AdditionalPluginDirectories drives this).
		string ThirdPartyDir = Path.Combine(ModuleDirectory, "ThirdParty");
		string BundledInclude = Path.Combine(ThirdPartyDir, "include");
		if (Directory.Exists(BundledInclude))
		{
			PublicIncludePaths.Add(BundledInclude);
		}
		else
		{
			string RepoRoot = Path.GetFullPath(Path.Combine(
				ModuleDirectory, "..", "..", "..", "..", "..", ".."));
			string FfiInclude = Path.Combine(RepoRoot, "crates", "citadel-client-ffi", "include");
			if (Directory.Exists(FfiInclude))
			{
				PublicIncludePaths.Add(FfiInclude);
			}
		}

		// Native-lib wiring, drop-in first. Priority:
		//   1) Compile-verification (CITADEL_FFI_STUB=1): compile the in-tree stub
		//      TU so the module links with no external lib. Used by
		//      ue-plugin-build.sh; NEVER set for a real game build.
		//   2) Bundled lib inside the plugin — <Module>/ThirdParty/<Platform>/
		//      citadel_client_ffi.lib. This makes a copied plugin link the real
		//      client with NO env vars. Populated by clients/unreal/bundle-ffi.sh
		//      (dev) or the release package (CI); gitignored.
		//   3) Consumer-supplied CITADEL_FFI_LIB=<abs path> fallback.
		bool bUseStub = string.Equals(
			Environment.GetEnvironmentVariable("CITADEL_FFI_STUB"), "1", StringComparison.Ordinal);
		string FfiLib = Environment.GetEnvironmentVariable("CITADEL_FFI_LIB");

		if (bUseStub)
		{
			PublicDefinitions.Add("CITADEL_FFI_STUB=1");
		}
		else
		{
			PublicDefinitions.Add("CITADEL_FFI_STUB=0");

			string PlatformDir =
				Target.Platform == UnrealTargetPlatform.Win64 ? "Win64" :
				Target.Platform == UnrealTargetPlatform.Mac ? "Mac" :
				Target.Platform == UnrealTargetPlatform.Linux ? "Linux" :
				Target.Platform.ToString();
			// Keep the platform-native archive suffix. MSVC consumes `.lib`; clang
			// on macOS/Linux consumes the Rust staticlib's `.a` archive. The
			// packages deliberately do not rename an archive across those formats.
			string BundledLibName =
				Target.Platform == UnrealTargetPlatform.Win64 ? "citadel_client_ffi.lib" :
				"libcitadel_client_ffi.a";
			string BundledLib = Path.Combine(ThirdPartyDir, PlatformDir, BundledLibName);

			string ResolvedLib =
				File.Exists(BundledLib) ? BundledLib :
				(!string.IsNullOrEmpty(FfiLib) && File.Exists(FfiLib) ? FfiLib : null);

			if (!string.IsNullOrEmpty(ResolvedLib))
			{
				PublicAdditionalLibraries.Add(ResolvedLib);
				// A Rust staticlib built with tokio/quinn pulls in these Windows
				// system import libs (cargo `--print native-static-libs`).
				if (Target.Platform == UnrealTargetPlatform.Win64)
				{
					PublicSystemLibraries.AddRange(new string[]
					{
						"bcrypt.lib", "advapi32.lib", "kernel32.lib", "ntdll.lib",
						"userenv.lib", "ws2_32.lib", "dbghelp.lib",
					});
				}
			}
			else
			{
				// No lib available — the citadel_* calls will fail to link until a
				// lib is bundled (run clients/unreal/bundle-ffi.sh) or CITADEL_FFI_LIB
				// is set. Warn loudly rather than silently mislink.
				System.Console.WriteLine(
					"warning: CitadelClient: no native FFI lib found "
					+ "(bundle ThirdParty/<Platform>/ with the native FFI archive via "
					+ "clients/unreal/bundle-ffi.sh, or set CITADEL_FFI_LIB).");
			}
		}
	}
}
