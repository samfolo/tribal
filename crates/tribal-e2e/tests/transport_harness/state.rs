//! Database pool, application state, and auth seeding for transport tests.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tribal_common::sha256_hex;
use tribal_config::{DEFAULT_OLLAMA_BASE_URL, ProviderKind, ServerConfig, WorkerConfig};
use tribal_db::{
    AuthTokenRepository, NewAuthToken, NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{PromptVersionId, Scope, full_access_scopes};
use tribal_inference::{ProviderRegistry, RequestClass};
use tribal_mcp::{ActivePromptVersions, AppState};
use tribal_test_utils::{MockEmbeddingProvider, MockInferenceProvider, test_context};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default instance ID prefix for transport tests.
const TEST_INSTANCE_ID: &str = "transport-test-00000000-0000-0000-0000-000000000000";

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

/// Seeds a principal and auth token with full access scopes.
pub async fn seed_auth(
    pool: &sqlx::PgPool,
    principal_key: &str,
    raw_token: &str,
    expires_in: chrono::Duration,
) {
    seed_scoped_auth(
        pool,
        principal_key,
        raw_token,
        expires_in,
        full_access_scopes(),
    )
    .await;
}

/// Seeds a principal and auth token with the given scopes.
pub async fn seed_scoped_auth(
    pool: &sqlx::PgPool,
    principal_key: &str,
    raw_token: &str,
    expires_in: chrono::Duration,
    scopes: Vec<Scope>,
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
                .scopes(scopes)
                .expires_at(Utc::now() + expires_in)
                .build(),
        )
        .await
        .expect("insert auth token");
}
