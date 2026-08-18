use alloy_primitives::{Address, B256, keccak256};
use alloy_rpc_types_eth::Filter;
use evm_fork_cache::reactive::{BlockInterest, LogInterest, ReactiveInterest, RouteKeySpec};
use evm_fork_cache_event_protocol::v1::{BlockMode, portable_interest};
use evm_fork_cache_remote::compile_portable_interests;

#[test]
fn portable_compiler_keeps_provider_filters_and_drops_local_routing_metadata() {
    let address = Address::repeat_byte(0x11);
    let topic0 = keccak256(b"Swap(address)");
    let interests: Vec<ReactiveInterest> = vec![
        ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(address).event_signature(topic0),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        }),
        ReactiveInterest::Blocks(BlockInterest::default()),
    ];

    let portable = compile_portable_interests(&interests).expect("portable interests");

    let portable_interest::Kind::Log(log) = portable[0].kind.as_ref().expect("log kind") else {
        panic!("first interest should be a log")
    };
    assert_eq!(log.addresses, vec![address.as_slice()]);
    assert_eq!(log.topics.len(), 1);
    assert_eq!(log.topics[0].values, vec![topic0.as_slice()]);
    let portable_interest::Kind::Block(block) = portable[1].kind.as_ref().expect("block kind")
    else {
        panic!("second interest should be a block")
    };
    assert_eq!(BlockMode::try_from(block.mode), Ok(BlockMode::Header));

    let _: B256 = topic0;
}
