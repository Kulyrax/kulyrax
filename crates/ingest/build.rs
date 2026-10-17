use std::path::PathBuf;

fn main() {
    // Vendor a `protoc` so no system install is needed (Phase 20).
    // SAFETY: single-threaded build script.
    unsafe {
        std::env::set_var(
            "PROTOC",
            protoc_bin_vendored::protoc_bin_path().expect("vendored protoc"),
        );
    }

    // Use this crate's own copy (`proto/`), not the workspace-root one — a
    // `cargo publish`'d package can only see files inside its own directory
    // (Phase 21.1). The two copies are kept in sync by hand; see the comment
    // atop `proto/kulyrax/v1/ingest.proto` at the workspace root.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto = manifest_dir.join("proto/kulyrax/v1/ingest.proto");
    let include = manifest_dir.join("proto");

    println!("cargo:rerun-if-changed={}", proto.display());

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &[include])
        .expect("compile ingest.proto");
}
