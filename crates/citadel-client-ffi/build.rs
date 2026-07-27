//! Generate the C header for the FFI surface with cbindgen.
//!
//! Writes `include/citadel_client.h` (committed for engine consumers who do not
//! build the crate). Best-effort: if generation fails we print a warning and let
//! the build continue, so header tooling never breaks the build.

use std::path::PathBuf;

fn main() {
    let crate_dir = match std::env::var("CARGO_MANIFEST_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => return,
    };
    let out = crate_dir.join("include").join("citadel_client.h");

    // Only regenerate when the source or config changes.
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/codec_ffi.rs");
    println!("cargo:rerun-if-changed=src/transform_ffi.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");

    let config = match cbindgen::Config::from_file(crate_dir.join("cbindgen.toml")) {
        Ok(c) => c,
        Err(e) => {
            println!("cargo:warning=cbindgen config error: {e}");
            return;
        }
    };

    match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
    {
        Ok(bindings) => {
            if let Some(parent) = out.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            bindings.write_to_file(&out);
        }
        Err(e) => {
            println!("cargo:warning=cbindgen header generation failed: {e}");
        }
    }
}
