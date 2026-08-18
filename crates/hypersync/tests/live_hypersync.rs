use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        ApplyDesiredState, LogInterest, OwnerInterests, PortableInterest, chain_event, delivery,
        portable_interest,
    },
};
use evm_fork_cache_hypersync::{ChainDataSource, HyperSyncDataSource, SourceEngine};
use futures::StreamExt;
use std::time::{Duration, Instant};

// `hypersync-client` uses Tokio's blocking section while establishing its
// verified HTTPS connection, so live coverage must exercise the production
// multi-thread runtime flavor.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires ENVIO_API_TOKEN and live HyperSync access"]
async fn live_hypersync_page_normalizes_compact_progress_into_an_acknowledgeable_batch() {
    let token = std::env::var("ENVIO_API_TOKEN").expect("ENVIO_API_TOKEN");
    let chain_id = std::env::var("HYPERSYNC_TEST_CHAIN_ID")
        .map_or(Ok(1_u64), |value| value.parse())
        .expect("HYPERSYNC_TEST_CHAIN_ID");
    let source = HyperSyncDataSource::new(chain_id, token).expect("HyperSync client");
    let height_started = Instant::now();
    let archive_height = source.height().await.expect("archive height");
    let height_elapsed = height_started.elapsed();
    let from_block = archive_height.saturating_sub(2);
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "live-smoke".into(),
        chain_id,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "logs".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
                    addresses: Vec::new(),
                    topics: Vec::new(),
                })),
            }],
            backfill: None,
            canonical: false,
        }],
    };
    let mut engine = SourceEngine::new(source, desired_state, from_block, from_block, 16);
    let query_started = Instant::now();
    let batch = engine
        .next_batch(archive_height)
        .await
        .expect("live HyperSync query")
        .expect("recent archive batch");
    let query_elapsed = query_started.elapsed();
    assert!(batch.cursor.as_ref().expect("cursor").next_block > from_block);
    let events = match batch.payload.as_ref().expect("delivery payload") {
        delivery::Payload::Data(data) => &data.records,
        other => panic!("expected data delivery, got {other:?}"),
    };
    assert!(!events.is_empty());
    assert!(events.iter().any(|record| {
        matches!(
            record.event.as_ref().and_then(|event| event.event.as_ref()),
            Some(chain_event::Event::BlockProgress(_))
        )
    }));
    engine
        .acknowledge(&batch.delivery_token)
        .expect("commit live batch");
    println!(
        "chain_id={chain_id} archive_height={archive_height} height_lookup_ms={:.2} query_normalize_ms={:.2} events={}",
        height_elapsed.as_secs_f64() * 1_000.0,
        query_elapsed.as_secs_f64() * 1_000.0,
        events.len(),
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires ENVIO_API_TOKEN and live HyperSync access"]
async fn live_hypersync_height_stream_emits_archive_progress() {
    let token = std::env::var("ENVIO_API_TOKEN").expect("ENVIO_API_TOKEN");
    let chain_id = std::env::var("HYPERSYNC_TEST_CHAIN_ID")
        .map_or(Ok(1_u64), |value| value.parse())
        .expect("HYPERSYNC_TEST_CHAIN_ID");
    let source = HyperSyncDataSource::new(chain_id, token).expect("HyperSync client");
    let mut heights = ChainDataSource::height_stream(&source).expect("height stream");

    let stream_started = Instant::now();
    let height = tokio::time::timeout(Duration::from_secs(15), heights.next())
        .await
        .expect("height stream timeout")
        .expect("height stream ended");
    assert!(height > 0);
    println!(
        "chain_id={chain_id} first_stream_height={height} first_height_ms={:.2}",
        stream_started.elapsed().as_secs_f64() * 1_000.0,
    );
}
