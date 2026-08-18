use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use evm_fork_cache_event_protocol::MAX_MESSAGE_SIZE_BYTES;
use evm_fork_cache_hypersync::{
    EventService, EventServiceLimits, HyperSyncSourceFactory, MAX_BLOCKS_PER_RESPONSE,
    MAX_DELIVERY_SIZE_BYTES, MAX_DYNAMIC_BYTES_PER_RESPONSE, MAX_LOGS_PER_RESPONSE,
    ManagedEventProvider, SessionAuthorizer, SessionStore, SourceResponseLimits,
};
use tokio::sync::Mutex;
use tonic::{
    metadata::MetadataMap,
    transport::{Identity, ServerTlsConfig},
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_REORG_DEPTH: usize = 64;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    if config.trusted_mesh
        && !config.listen_address.ip().is_loopback()
        && !(config.tls_identity.is_some() && config.bearer_token.is_some())
    {
        warn!(
            address = %config.listen_address,
            "non-loopback listener is relying on the explicitly configured trusted service mesh for missing transport encryption or client authentication"
        );
    }
    let store = Arc::new(Mutex::new(
        SessionStore::open(&config.database_path).with_context(|| {
            format!(
                "open session database at {}",
                config.database_path.display()
            )
        })?,
    ));
    let provider = ManagedEventProvider::new(
        HyperSyncSourceFactory::new(config.api_token),
        config.reorg_depth,
    )
    .with_request_timeout(config.source_request_timeout)
    .context("configure source request timeout")?
    .with_max_delivery_bytes(config.source_delivery_bytes)
    .context("configure source delivery size limit")?
    .with_response_limits(config.response_limits)
    .with_max_resident_sessions(config.max_resident_sessions)
    .context("configure managed source session limit")?;
    let provider = Arc::new(provider);
    let mut limits = EventServiceLimits::default();
    limits.max_delivery_bytes = config.max_delivery_bytes;
    limits.max_active_sessions = config.max_resident_sessions;
    limits.max_persisted_sessions = config.max_persisted_sessions;
    let mut service = EventService::new(store, provider, config.poll_interval)
        .context("configure event service")?
        .with_limits(limits)
        .with_source_operation_timeout(config.source_request_timeout)
        .context("configure service source-operation timeout")?;
    if let Some(token) = config.bearer_token {
        service = service.with_authorizer(Arc::new(BearerAuthorizer::new(token)));
    }
    let metrics = service.metrics();
    let service_shutdown = service.shutdown_handle();

    info!(address = %config.listen_address, "starting HyperSync event service");
    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    if let Some((certificate, private_key)) = config.tls_identity {
        server = server
            .tls_config(
                ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key)),
            )
            .context("configure event service TLS identity")?;
    }
    server
        .add_service(service.into_server())
        .serve_with_shutdown(config.listen_address, async move {
            shutdown_signal().await;
            service_shutdown.shutdown();
        })
        .await
        .context("serve event stream")?;
    let snapshot = metrics.snapshot();
    info!(
        sessions = snapshot.sessions_accepted,
        deliveries = snapshot.deliveries_persisted,
        acknowledgements = snapshot.acknowledgements_committed,
        source_errors = snapshot.source_errors,
        "event service stopped"
    );
    Ok(())
}

struct Config {
    listen_address: SocketAddr,
    database_path: PathBuf,
    api_token: String,
    reorg_depth: usize,
    poll_interval: Duration,
    source_request_timeout: Duration,
    max_delivery_bytes: usize,
    source_delivery_bytes: usize,
    response_limits: SourceResponseLimits,
    max_resident_sessions: usize,
    max_persisted_sessions: usize,
    bearer_token: Option<String>,
    tls_identity: Option<(Vec<u8>, Vec<u8>)>,
    trusted_mesh: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        let listen_address = std::env::var("EVM_FORK_CACHE_EVENT_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:50051".into())
            .parse()
            .context("parse EVM_FORK_CACHE_EVENT_LISTEN")?;
        let database_path = std::env::var_os("EVM_FORK_CACHE_EVENT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("evm-fork-cache-events.sqlite"));
        let api_token = read_required_secret("ENVIO_API_TOKEN")?;
        let reorg_depth = parse_env("EVM_FORK_CACHE_REORG_DEPTH", DEFAULT_REORG_DEPTH)?;
        validate_reorg_depth(reorg_depth)?;
        let poll_interval_ms = parse_env("EVM_FORK_CACHE_POLL_INTERVAL_MS", 1_000_u64)?;
        if poll_interval_ms == 0 {
            anyhow::bail!("EVM_FORK_CACHE_POLL_INTERVAL_MS must be greater than zero");
        }
        let poll_interval = Duration::from_millis(poll_interval_ms);
        let source_request_timeout_ms =
            parse_env("EVM_FORK_CACHE_SOURCE_REQUEST_TIMEOUT_MS", 45_000_u64)?;
        if source_request_timeout_ms == 0 {
            anyhow::bail!("EVM_FORK_CACHE_SOURCE_REQUEST_TIMEOUT_MS must be greater than zero");
        }
        let source_request_timeout = Duration::from_millis(source_request_timeout_ms);
        let max_delivery_bytes = parse_env(
            "EVM_FORK_CACHE_EVENT_MAX_DELIVERY_BYTES",
            MAX_MESSAGE_SIZE_BYTES,
        )?;
        let envelope_reserve = MAX_MESSAGE_SIZE_BYTES - MAX_DELIVERY_SIZE_BYTES;
        if max_delivery_bytes <= envelope_reserve || max_delivery_bytes > MAX_MESSAGE_SIZE_BYTES {
            anyhow::bail!(
                "EVM_FORK_CACHE_EVENT_MAX_DELIVERY_BYTES must be within {}..={MAX_MESSAGE_SIZE_BYTES}",
                envelope_reserve + 1
            );
        }
        let source_delivery_bytes = max_delivery_bytes - envelope_reserve;
        let response_limits = SourceResponseLimits::new(
            parse_env(
                "EVM_FORK_CACHE_SOURCE_MAX_RESPONSE_BLOCKS",
                MAX_BLOCKS_PER_RESPONSE,
            )?,
            parse_env(
                "EVM_FORK_CACHE_SOURCE_MAX_RESPONSE_LOGS",
                MAX_LOGS_PER_RESPONSE,
            )?,
            parse_env(
                "EVM_FORK_CACHE_SOURCE_MAX_RESPONSE_DYNAMIC_BYTES",
                MAX_DYNAMIC_BYTES_PER_RESPONSE,
            )?,
        )
        .context("configure hard source response limits")?;
        let max_resident_sessions =
            parse_env("EVM_FORK_CACHE_SOURCE_MAX_RESIDENT_SESSIONS", 4_096_usize)?;
        if max_resident_sessions == 0 {
            anyhow::bail!("EVM_FORK_CACHE_SOURCE_MAX_RESIDENT_SESSIONS must be greater than zero");
        }
        let max_persisted_sessions =
            parse_env("EVM_FORK_CACHE_EVENT_MAX_PERSISTED_SESSIONS", 65_536_usize)?;
        if max_persisted_sessions == 0 {
            anyhow::bail!("EVM_FORK_CACHE_EVENT_MAX_PERSISTED_SESSIONS must be greater than zero");
        }
        let bearer_token = read_optional_secret("EVM_FORK_CACHE_EVENT_BEARER_TOKEN")?;
        let tls_certificate = std::env::var_os("EVM_FORK_CACHE_EVENT_TLS_CERT");
        let tls_private_key = std::env::var_os("EVM_FORK_CACHE_EVENT_TLS_KEY");
        let tls_identity = match (tls_certificate, tls_private_key) {
            (Some(certificate), Some(private_key)) => Some((
                std::fs::read(&certificate).with_context(|| {
                    format!(
                        "read TLS certificate {}",
                        PathBuf::from(certificate).display()
                    )
                })?,
                std::fs::read(&private_key).with_context(|| {
                    format!(
                        "read TLS private key {}",
                        PathBuf::from(private_key).display()
                    )
                })?,
            )),
            (None, None) => None,
            _ => anyhow::bail!(
                "EVM_FORK_CACHE_EVENT_TLS_CERT and EVM_FORK_CACHE_EVENT_TLS_KEY must be set together"
            ),
        };
        let trusted_mesh = parse_env("EVM_FORK_CACHE_EVENT_TRUSTED_MESH", false)?;
        validate_listener_security(
            listen_address,
            tls_identity.is_some(),
            bearer_token.is_some(),
            trusted_mesh,
        )?;
        Ok(Self {
            listen_address,
            database_path,
            api_token,
            reorg_depth,
            poll_interval,
            source_request_timeout,
            max_delivery_bytes,
            source_delivery_bytes,
            response_limits,
            max_resident_sessions,
            max_persisted_sessions,
            bearer_token,
            tls_identity,
            trusted_mesh,
        })
    }
}

fn validate_listener_security(
    listen_address: SocketAddr,
    has_tls_identity: bool,
    has_bearer_token: bool,
    trusted_mesh: bool,
) -> Result<()> {
    if !(listen_address.ip().is_loopback() || trusted_mesh || has_tls_identity && has_bearer_token)
    {
        anyhow::bail!(
            "non-loopback EVM_FORK_CACHE_EVENT_LISTEN requires both direct TLS and bearer authentication, or explicit EVM_FORK_CACHE_EVENT_TRUSTED_MESH=true"
        );
    }
    Ok(())
}

fn validate_reorg_depth(reorg_depth: usize) -> Result<()> {
    if reorg_depth == 0 {
        anyhow::bail!("EVM_FORK_CACHE_REORG_DEPTH must be greater than zero");
    }
    Ok(())
}

struct BearerAuthorizer {
    expected: Vec<u8>,
}

impl BearerAuthorizer {
    fn new(token: String) -> Self {
        Self {
            expected: format!("Bearer {token}").into_bytes(),
        }
    }
}

impl SessionAuthorizer for BearerAuthorizer {
    fn authorize(&self, metadata: &MetadataMap) -> Result<(), String> {
        let supplied = metadata
            .get("authorization")
            .map(|value| value.as_encoded_bytes())
            .unwrap_or_default();
        if constant_time_eq(supplied, &self.expected) {
            Ok(())
        } else {
            Err("invalid bearer authorization".into())
        }
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let width = left.len().max(right.len());
    for index in 0..width {
        difference |= usize::from(left.get(index).copied().unwrap_or_default())
            ^ usize::from(right.get(index).copied().unwrap_or_default());
    }
    difference == 0
}

fn parse_env<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    std::env::var(name).map_or_else(
        |_| Ok(default),
        |value| value.parse().with_context(|| format!("parse {name}")),
    )
}

fn read_required_secret(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("{name} is required"))?;
    validate_required_secret(name, value)
}

fn read_optional_secret(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => validate_optional_secret(name, Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("{name} must contain valid UTF-8")
        }
    }
}

fn validate_required_secret(name: &str, value: String) -> Result<String> {
    if value.trim().is_empty() {
        anyhow::bail!("{name} must not be empty or whitespace-only");
    }
    Ok(value)
}

fn validate_optional_secret(name: &str, value: Option<String>) -> Result<Option<String>> {
    value
        .map(|value| validate_required_secret(name, value))
        .transpose()
}

async fn first_shutdown<C, T>(ctrl_c: C, terminate: Option<T>)
where
    C: std::future::Future<Output = ()>,
    T: std::future::Future<Output = ()>,
{
    if let Some(terminate) = terminate {
        tokio::select! {
            () = ctrl_c => {}
            () = terminate => {}
        }
    } else {
        ctrl_c.await;
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .ok()
            .map(|mut signal| async move {
                let _ = signal.recv().await;
            });
        first_shutdown(ctrl_c, terminate).await;
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        DEFAULT_REORG_DEPTH, first_shutdown, validate_listener_security, validate_optional_secret,
        validate_reorg_depth,
    };

    #[test]
    fn default_reorg_depth_matches_the_core_runtime_journal() {
        assert_eq!(DEFAULT_REORG_DEPTH, 64);
    }

    #[test]
    fn zero_reorg_depth_is_rejected_instead_of_silently_clamped() {
        let error = validate_reorg_depth(0).expect_err("zero depth is unsafe");
        assert!(error.to_string().contains("EVM_FORK_CACHE_REORG_DEPTH"));
        validate_reorg_depth(1).expect("positive depth");
    }

    #[test]
    fn optional_bearer_secret_rejects_empty_and_whitespace_values() {
        assert!(
            validate_optional_secret("TOKEN", None)
                .expect("unset is optional")
                .is_none()
        );
        for value in ["", " ", "\t\r\n"] {
            let error = validate_optional_secret("TOKEN", Some(value.into()))
                .expect_err("present bearer values must contain a non-whitespace byte");
            assert!(error.to_string().contains("TOKEN"));
        }
        assert_eq!(
            validate_optional_secret("TOKEN", Some("secret".into())).expect("nonempty token"),
            Some("secret".into())
        );
    }

    #[test]
    fn non_loopback_listener_requires_complete_direct_security_or_a_trusted_mesh() {
        let loopback = "127.0.0.1:50051".parse().expect("loopback address");
        validate_listener_security(loopback, false, false, false)
            .expect("loopback is safe by default");

        let remote = "0.0.0.0:50051".parse().expect("wildcard address");
        let error = validate_listener_security(remote, false, false, false)
            .expect_err("implicit remote plaintext must fail closed");
        assert!(error.to_string().contains("TRUSTED_MESH"));
        validate_listener_security(remote, true, false, false)
            .expect_err("server TLS without client authentication is insufficient");
        validate_listener_security(remote, false, true, false)
            .expect_err("a bearer token must not cross a plaintext network boundary");
        validate_listener_security(remote, true, true, false)
            .expect("direct TLS plus bearer authentication");
        validate_listener_security(remote, false, false, true).expect("explicit trusted mesh");
    }

    #[tokio::test]
    async fn shutdown_completes_when_the_termination_future_fires() {
        tokio::time::timeout(
            Duration::from_secs(1),
            first_shutdown(std::future::pending(), Some(std::future::ready(()))),
        )
        .await
        .expect("termination should win over a pending Ctrl-C future");
    }
}
