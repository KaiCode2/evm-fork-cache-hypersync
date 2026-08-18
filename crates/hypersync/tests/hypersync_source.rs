use evm_fork_cache_hypersync::HyperSyncDataSource;

#[test]
fn hypersync_source_builds_without_network_access_for_a_supported_chain() {
    let source = HyperSyncDataSource::new(1, "00000000-0000-0000-0000-000000000000")
        .expect("valid chain and token shape should configure the client");

    assert_eq!(source.chain_id(), 1);
}
