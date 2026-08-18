fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc);

    tonic_prost_build::configure().compile_with_config(
        prost,
        &["proto/evm_fork_cache_events_v1.proto"],
        &["proto"],
    )?;
    println!("cargo:rerun-if-changed=proto/evm_fork_cache_events_v1.proto");
    Ok(())
}
