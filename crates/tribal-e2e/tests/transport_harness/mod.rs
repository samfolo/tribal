// mod.rs is required here — cargo discovers top-level files in tests/ as
// separate binaries, so transport_harness.rs would conflict with the
// transport_harness/ directory.
//
// Shared helpers for HTTP and SSE transport integration tests.
#![allow(dead_code)]

use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use chrono::Utc;
use dashmap::DashMap;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tokio_util::sync::CancellationToken;
use tribal_common::sha256_hex;
use tribal_config::{DEFAULT_OLLAMA_BASE_URL, ProviderKind, ServerConfig, WorkerConfig};
use tribal_db::{
    AuthTokenRepository, NewAuthToken, NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{PromptVersionId, full_access_scopes};
use tribal_inference::{ProviderRegistry, RequestClass};
use tribal_mcp::{ActivePromptVersions, AppState};
use tribal_test_utils::{MockEmbeddingProvider, MockInferenceProvider, test_context};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default instance ID prefix for transport tests.
const TEST_INSTANCE_ID: &str = "transport-test-00000000-0000-0000-0000-000000000000";

/// Duration used when a lifecycle timeout should never fire during
/// a test.
pub const LIFECYCLE_FAR_FUTURE_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// Creates a fresh pool against the shared test database.
///
/// Each test gets its own pool so transport shutdown in one test
/// does not starve connections in the next.
pub async fn fresh_pool() -> sqlx::PgPool {
    let ctx = test_context().await;
    sqlx::pool::PoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(ctx.database_url())
        .await
        .expect("connect fresh pool")
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

/// Builds an [`AppState`] backed by a real test database pool and mock
/// providers.
pub fn test_app_state(pool: sqlx::PgPool, ct: CancellationToken) -> Arc<AppState> {
    let provider_kind = ProviderKind::default().to_string();

    let embedding_key = tribal_inference::ProviderKey::new(
        &provider_kind,
        DEFAULT_OLLAMA_BASE_URL,
        RequestClass::Embedding,
    )
    .expect("test embedding key");

    let inference_key = tribal_inference::ProviderKey::new(
        &provider_kind,
        DEFAULT_OLLAMA_BASE_URL,
        RequestClass::Inference,
    )
    .expect("test inference key");

    Arc::new(
        AppState::builder()
            .pool_mcp(pool.clone())
            .pool_worker(pool)
            .instance_id(Arc::from(TEST_INSTANCE_ID))
            .active_prompt_versions(Arc::new(RwLock::new(ActivePromptVersions::new(
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
            ))))
            .provider_registry(Arc::new(
                ProviderRegistry::new(Vec::new())
                    .expect("empty registry construction must not fail"),
            ))
            .embedding_provider(Arc::new(MockEmbeddingProvider::builder().build()))
            .extraction_provider(Arc::new(MockInferenceProvider::builder().build()))
            .triage_provider(Arc::new(MockInferenceProvider::builder().build()))
            .relation_provider(Arc::new(MockInferenceProvider::builder().build()))
            .embedding_key(embedding_key)
            .extraction_key(inference_key.clone())
            .triage_key(inference_key.clone())
            .relation_key(inference_key)
            .worker_config(WorkerConfig::default())
            .server_config(Arc::new(ServerConfig::default()))
            .cancellation_token(ct)
            .job_state_txs(Arc::new(DashMap::new()))
            .build(),
    )
}

// ---------------------------------------------------------------------------
// Auth seeding
// ---------------------------------------------------------------------------

/// Seeds a principal and auth token with the given raw token value and
/// expiry offset from now.
pub async fn seed_auth(
    pool: &sqlx::PgPool,
    principal_key: &str,
    raw_token: &str,
    expires_in: chrono::Duration,
) {
    let mut conn = pool.acquire().await.expect("acquire connection");

    let principal = PgPrincipalRepository
        .insert(
            &mut conn,
            &NewPrincipal::builder()
                .principal_key(principal_key.to_owned())
                .build(),
        )
        .await
        .expect("insert principal");

    PgAuthTokenRepository
        .insert(
            &mut conn,
            &NewAuthToken::builder()
                .token_hash(sha256_hex(raw_token))
                .principal_id(principal.id())
                .scopes(full_access_scopes())
                .expires_at(Utc::now() + expires_in)
                .build(),
        )
        .await
        .expect("insert auth token");
}

// ---------------------------------------------------------------------------
// Transport handle
// ---------------------------------------------------------------------------

/// Handle to a running transport, ensuring clean shutdown.
pub struct TransportHandle {
    pub addr: SocketAddr,
    ct: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl TransportHandle {
    /// Cancels the transport and waits for the task to finish, ensuring
    /// all pool connections are released before the next test.
    pub async fn shutdown(self) {
        self.ct.cancel();
        self.join.await.expect("transport task must not panic");
    }
}

/// Spawns a transport using the provided runner closure and returns a
/// handle for clean teardown.
///
/// The `runner` receives a `CancellationToken`, a `ServerConfig`, and
/// a pre-bound `TcpListener`, and returns a future that runs the
/// transport until shutdown.
pub async fn spawn_transport<F, Fut>(
    ct: CancellationToken,
    server_config: ServerConfig,
    runner: F,
) -> TransportHandle
where
    F: FnOnce(CancellationToken, ServerConfig, TcpListener) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local address");

    let task_ct = ct.clone();
    let join = tokio::spawn(async move {
        runner(task_ct, server_config, listener).await;
    });

    // Wait until the server is accepting connections.
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return TransportHandle { addr, ct, join };
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("transport did not become ready within 500ms");
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

/// Builds a reqwest client configured for transport tests.
///
/// Disables connection pooling and proxies to avoid interference
/// between tests.
pub fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build reqwest client")
}
