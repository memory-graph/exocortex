// crates/exocortex-wire/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/ingest.proto",
                "../../proto/cluster.proto",
                "../../proto/sse.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
