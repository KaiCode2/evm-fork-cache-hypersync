#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod desired_state;
mod service;
mod session_store;

pub use desired_state::{DesiredStateError, DesiredStateRegistry, validate_desired_state};
pub use service::{
    DeliveryRequest, EventService, EventServiceConfigError, EventServiceLimits,
    EventServiceMetrics, EventServiceMetricsSnapshot, EventServiceShutdown, EventSource,
    EventSourceError, EventSourceErrorKind, PreparationId, SessionAuthorizer,
};
pub use session_store::{
    PersistedSession, SESSION_SCHEMA_VERSION, SessionStore, SessionStoreError,
};
