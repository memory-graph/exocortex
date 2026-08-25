// crates/exocortex-wire/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Manifest-relative so the crate builds both from the repo and from
    // the published tarball (protos ship inside the package).
    let dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_dir = std::path::Path::new(&dir).join("proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                proto_dir.join("ingest.proto"),
                proto_dir.join("cluster.proto"),
                proto_dir.join("sse.proto"),
            ],
            &[proto_dir],
        )?;
    Ok(())
}
