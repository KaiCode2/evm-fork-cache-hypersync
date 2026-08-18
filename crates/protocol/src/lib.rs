#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum encoded gRPC message size accepted by both client and service.
pub const MAX_MESSAGE_SIZE_BYTES: usize = 32 * 1024 * 1024;

/// Version 1 protobuf messages and gRPC service definitions.
#[allow(missing_docs, clippy::missing_errors_doc)]
pub mod v1 {
    tonic::include_proto!("evm_fork_cache.events.v1");
}
