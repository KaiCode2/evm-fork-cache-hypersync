#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod canonical;
mod hypersync_source;
mod managed_provider;
mod normalize;
mod page_validation;
mod query_plan;
mod source_engine;

pub use canonical::{CanonicalError, CanonicalTracker, ReorgDelta};
pub use evm_fork_cache_event_service::{
    DeliveryRequest, DesiredStateError, DesiredStateRegistry, EventService,
    EventServiceConfigError, EventServiceLimits, EventServiceMetrics, EventServiceMetricsSnapshot,
    EventSource, EventSourceError, EventSourceErrorKind, PersistedSession, SessionAuthorizer,
    SessionStore, SessionStoreError,
};
pub use hypersync_source::HyperSyncDataSource;
pub use managed_provider::{ChainDataSourceFactory, HyperSyncSourceFactory, ManagedEventProvider};
pub use normalize::{NormalizeError, SourcePage, normalize_page_unchecked};
pub use page_validation::SourcePageError;
pub use query_plan::{
    MAX_BLOCKS_PER_QUERY, MAX_COMPILED_LOG_FILTERS, MAX_LOGS_PER_QUERY, QueryPlanError,
    compile_query,
};
pub use source_engine::{
    ChainDataSource, ChainHeightStream, MAX_BLOCKS_PER_RESPONSE, MAX_DELIVERY_SIZE_BYTES,
    MAX_DYNAMIC_BYTES_PER_RESPONSE, MAX_LOGS_PER_RESPONSE, SourceEngine, SourceEngineError,
    SourceError, SourceResponseLimits, SourceResume,
};
