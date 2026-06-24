//! Database pool, application state, and auth seeding for transport tests.

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tribal_common::sha256_hex;
use tribal_config::{ServerConfig, WorkerConfig};
use tribal_db::{
    AuthTokenRepository, NewAuthToken, NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{PromptVersionId, ProviderKind, Scope, full_access_scopes};
use tribal_inference::{
    CompletionStageSpec, CompletionStageSpecs, EmptyCredentialResolver, InferenceGateway,
    InjectedCompletion, InjectedProviders, NoopLedgerSink, ProviderIdentity, ProviderRegistry,
    RequestClass,
};
use tribal_mcp::{ActivePromptVersions, AppState};
use tribal_telemetry::noop_recorder;
use tribal_test_utils::{MockInferenceProvider, TestDb};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default instance ID prefix for transport tests.
const TEST_INSTANCE_ID: &str = "transport-test-00000000-0000-0000-0000-000000000000";

/// Canonical resource URL the transport harness binds and audience-binds
/// seeded tokens to. The transport runtimes build their resource from
/// this so a seeded token's audience matches what the bearer middleware
/// verifies (RFC 8707).
pub const TEST_CANONICAL_RESOURCE: &str = "http://127.0.0.1:8080/mcp";

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// Provisions an owned, isolated test database.
///
/// Returns the [`TestDb`] so the caller keeps it alive for the whole test:
/// its `Drop` drops the cloned database, so it must outlive the pool, the
/// `AppState`, and the running transport. Obtain a pool via
/// [`TestDb::pool`]/[`TestDb::create_pool`].
pub async fn fresh_pool() -> TestDb {
    TestDb::new().await
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

/// Builds an [`AppState`] backed by a real test database pool and mock
/// providers.
pub fn test_app_state(pool: sqlx::PgPool, ct: CancellationToken) -> Arc<AppState> {
    let provider_kind = ProviderKind::default().to_string();

    let inference_key = tribal_inference::ProviderKey::new(
        &provider_kind,
        ProviderKind::DEFAULT_OLLAMA_BASE_URL,
        RequestClass::Inference,
    )
    .expect("test inference key");

    let registry =
        ProviderRegistry::new(Vec::new()).expect("empty registry construction must not fail");
    let gateway = Arc::new(InferenceGateway::with_providers(InjectedProviders {
        registry,
        extraction: InjectedCompletion {
            provider: Arc::new(MockInferenceProvider::builder().build()),
            key: inference_key.clone(),
        },
        triage: InjectedCompletion {
            provider: Arc::new(MockInferenceProvider::builder().build()),
            key: inference_key.clone(),
        },
        relation: InjectedCompletion {
            provider: Arc::new(MockInferenceProvider::builder().build()),
            key: inference_key,
        },
        embeddings: Vec::new(),
        credentials: Arc::new(EmptyCredentialResolver),
        sink: Arc::new(NoopLedgerSink),
    }));
    let embedding_identity = ProviderIdentity {
        name: provider_kind.clone(),
        model: "mock-embed".to_owned(),
    };

    Arc::new(
        AppState::builder()
            .pool_mcp(pool.clone())
            .pool_worker(pool)
            .instance_id(Arc::from(TEST_INSTANCE_ID))
            .build_version(Arc::from("test-build"))
            .stage_specs(test_stage_specs())
            .embedding_dimensions(768)
            .agents_config(tribal_config::AgentsConfig::default())
            .pipeline_parameters(tribal_domain::PipelineParameters::default())
            .active_prompt_versions(Arc::new(RwLock::new(ActivePromptVersions::new(
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
                PromptVersionId::new(),
            ))))
            .gateway(gateway)
            .embedding_identity(embedding_identity)
            .worker_config(WorkerConfig::default())
            .server_config(Arc::new(ServerConfig::default()))
            .cancellation_token(ct)
            .job_state_txs(Arc::new(DashMap::new()))
            .metrics(noop_recorder())
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
                .audience(TEST_CANONICAL_RESOURCE.to_owned())
                .expires_at(Utc::now() + expires_in)
                .build(),
        )
        .await
        .expect("insert auth token");
}

/// The boot-time stage specs the fingerprint computation reads: one mock
/// endpoint for all three stages.
fn test_stage_specs() -> CompletionStageSpecs {
    let spec = CompletionStageSpec {
        provider: ProviderKind::Ollama,
        model: "mock-model".to_owned(),
        base_url: "http://localhost:9999".to_owned(),
        api_key: String::new(),
        parameters: tribal_domain::StageParameters::default(),
    };
    CompletionStageSpecs {
        extraction: spec.clone(),
        triage: spec.clone(),
        relation: spec,
    }
}
