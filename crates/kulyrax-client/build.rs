use std::path::PathBuf;

fn main() {
    // SAFETY: single-threaded build script.
    unsafe {
        std::env::set_var(
            "PROTOC",
            protoc_bin_vendored::protoc_bin_path().expect("vendored protoc"),
        );
    }

    // Use this crate's own copy (`proto/`) — see the note in
    // `kulyrax-ingest/build.rs` and atop the workspace-root `.proto` file.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto = manifest_dir.join("proto/kulyrax/v1/ingest.proto");
    let include = manifest_dir.join("proto");

    println!("cargo:rerun-if-changed={}", proto.display());

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[proto], &[include])
        .expect("compile ingest.proto");
}
