//! First-boot provisioning of the genesis embedding profile.
//!
//! Run after migrations, this seeds the corpus's first embedding profile from
//! the `init.embedding` genesis seed and builds its per-profile partial HNSW
//! indexes. It is
//! serialised by the provisioning and embedding-profile authority locks, idempotent, and
//! crash-safe: a crash before the profile is marked `complete` leaves it
//! `building` (never active), so a restart re-adopts and completes it rather
//! than minting a second genesis. It never registers a later configuration
//! change as a profile; that is the reindex's job.

use sqlx::{PgConnection, PgPool};
use tribal_common::{embedding_profile_fingerprint, random_duration_in_range};
use tribal_config::TribalConfig;
use tribal_db::{
    EmbeddingIndexRepository, EmbeddingProfileRepository, EmbeddingTable, MigrationRepository,
    NewEmbeddingProfile, PgEmbeddingIndexRepository, PgEmbeddingProfileRepository,
    PgMigrationRepository, advisory_locks,
};
use tribal_domain::{DistanceMetric, EmbeddingProfile, normalise_endpoint_url};
use tribal_inference::resolve_dimensions;

use super::{
    POOL_NAME_MCP,
    constants::{MIGRATION_MAX_ATTEMPTS, MIGRATION_RETRY_SLEEP_MAX, MIGRATION_RETRY_SLEEP_MIN},
};
use crate::error::AppError;

/// Ensures the genesis embedding profile and its indexes exist.
///
/// Returns once a `complete` genesis profile is present, whether this process
/// created it or observed another process complete it.
///
/// # Errors
///
/// Returns an [`AppError`] when the pool, advisory lock, or the underlying
/// queries and index builds fail, or when the provisioning lock cannot be
/// acquired within the retry budget.
pub(crate) async fn provision_genesis(
    pool: &PgPool,
    config: &TribalConfig,
) -> Result<(), AppError> {
    let migration_repo = PgMigrationRepository;

    for attempt in 1..=MIGRATION_MAX_ATTEMPTS {
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| AppError::pool_acquire(POOL_NAME_MCP, "provisioning", e))?;

        // Already provisioned, by this process on a prior attempt or another
        // process: nothing to do.
        if PgEmbeddingProfileRepository
            .find_active(&mut conn)
            .await
            .map_err(database_error)?
            .is_some()
        {
            return Ok(());
        }

        let acquired = migration_repo
            .try_advisory_lock(&mut conn, advisory_locks::PROVISIONING)
            .await
            .map_err(database_error)?;

        if acquired {
            let profile_authority = migration_repo
                .try_advisory_lock(&mut conn, advisory_locks::EMBEDDING_PROFILE_AUTHORITY)
                .await
                .map_err(database_error)?;
            if !profile_authority {
                if let Err(error) = migration_repo
                    .release_advisory_lock(&mut conn, advisory_locks::PROVISIONING)
                    .await
                {
                    tracing::warn!(%error, "failed to release provisioning advisory lock");
                }
                if attempt < MIGRATION_MAX_ATTEMPTS {
                    tokio::time::sleep(random_duration_in_range(
                        MIGRATION_RETRY_SLEEP_MIN,
                        MIGRATION_RETRY_SLEEP_MAX,
                    ))
                    .await;
                    continue;
                }
                break;
            }
            let result = provision_under_lock(&mut conn, config).await;
            if let Err(error) = migration_repo
                .release_advisory_lock(&mut conn, advisory_locks::EMBEDDING_PROFILE_AUTHORITY)
                .await
            {
                tracing::warn!(%error, "failed to release embedding-profile authority");
            }
            if let Err(e) = migration_repo
                .release_advisory_lock(&mut conn, advisory_locks::PROVISIONING)
                .await
            {
                tracing::warn!(%e, "failed to release provisioning advisory lock");
            }
            return result;
        }

        // Another process holds the lock; wait and re-check.
        if attempt < MIGRATION_MAX_ATTEMPTS {
            tokio::time::sleep(random_duration_in_range(
                MIGRATION_RETRY_SLEEP_MIN,
                MIGRATION_RETRY_SLEEP_MAX,
            ))
            .await;
        }
    }

    Err(AppError::ProvisioningLockFailed {
        attempts: MIGRATION_MAX_ATTEMPTS,
    })
}

/// Provisions the genesis profile while holding the advisory lock.
async fn provision_under_lock(
    conn: &mut PgConnection,
    config: &TribalConfig,
) -> Result<(), AppError> {
    // Re-check under the lock: a complete profile means provisioning finished.
    if PgEmbeddingProfileRepository
        .find_active(conn)
        .await
        .map_err(database_error)?
        .is_some()
    {
        return Ok(());
    }

    // Adopt a genesis left by a prior crashed run, or insert a fresh one.
    let profile = match PgEmbeddingProfileRepository
        .find_building(conn)
        .await
        .map_err(database_error)?
    {
        Some(existing) => existing,
        None => insert_genesis(conn, config).await?,
    };

    // Build each table's per-profile partial HNSW index (idempotent via the
    // three-way state check). On a fresh install the tables are empty, so this
    // is trivial.
    for table in [EmbeddingTable::Embeddings, EmbeddingTable::TagEmbeddings] {
        PgEmbeddingIndexRepository
            .ensure_partial_hnsw(
                conn,
                table,
                profile.epoch(),
                profile.dimensions(),
                profile.id(),
            )
            .await
            .map_err(database_error)?;
    }

    PgEmbeddingProfileRepository
        .mark_complete(conn, profile.id())
        .await
        .map_err(database_error)?;

    Ok(())
}

/// Reads the active embedding profile, which provisioning guarantees exists
/// once it has returned. The active profile is the live embedding identity the
/// provider builders construct from.
///
/// # Errors
///
/// Returns an [`AppError`] when the pool or query fails, or when no active
/// profile exists (a provisioning invariant violation).
pub(crate) async fn read_active_profile(pool: &PgPool) -> Result<EmbeddingProfile, AppError> {
    let mut conn = pool.acquire().await.map_err(|e| {
        AppError::pool_acquire(POOL_NAME_MCP, "reading active embedding profile", e)
    })?;

    PgEmbeddingProfileRepository
        .find_active(&mut conn)
        .await
        .map_err(database_error)?
        .ok_or_else(|| AppError::ProviderSetup {
            context: "no active embedding profile after provisioning".to_owned(),
        })
}

/// Inserts a `building` genesis profile from the `init.embedding` seed.
async fn insert_genesis(
    conn: &mut PgConnection,
    config: &TribalConfig,
) -> Result<EmbeddingProfile, AppError> {
    let connection = config
        .provider_connections
        .require(&config.init.embedding.connection)
        .map_err(|source| AppError::ConfigInvariant {
            reason: source.to_string(),
        })?;
    let provider = connection.provider();
    let base_url = genesis_base_url(config)?;
    let normalised_base_url =
        normalise_endpoint_url(&base_url).map_err(|e| AppError::ConfigInvariant {
            reason: e.to_string(),
        })?;
    let model = config.init.embedding.model.clone();
    let dimensions = resolve_dimensions(provider, &model, config.init.embedding.dimensions)
        .map_err(|e| AppError::ConfigInvariant {
            reason: e.to_string(),
        })?;

    let fingerprint_hash = embedding_profile_fingerprint(
        provider.as_str(),
        &normalised_base_url,
        &model,
        dimensions,
        DistanceMetric::Cosine.as_str(),
        "",
    );

    let new_profile = NewEmbeddingProfile::builder()
        .provider_kind(provider)
        .normalised_base_url(normalised_base_url)
        .model(model)
        .dimensions(dimensions)
        .fingerprint_hash(fingerprint_hash)
        .build();

    PgEmbeddingProfileRepository
        .insert(conn, &new_profile)
        .await
        .map_err(database_error)
}

/// Resolves the genesis embedding endpoint from the `init.embedding` seed: the
/// configured URL, else the provider's built-in default, erroring when the
/// provider has no local endpoint.
fn genesis_base_url(config: &TribalConfig) -> Result<String, AppError> {
    let connection = config
        .provider_connections
        .require(&config.init.embedding.connection)
        .map_err(|source| AppError::ConfigInvariant {
            reason: source.to_string(),
        })?;
    let provider = connection.provider();
    connection
        .base_url()
        .or_else(|| provider.default_base_url())
        .map(ToOwned::to_owned)
        .ok_or_else(|| AppError::ProviderSetup {
            context: format!(
                "{provider} has no local endpoint: it is served through the \
                 managed gateway, not addressed by URL"
            ),
        })
}

fn database_error(source: tribal_db::DbError) -> AppError {
    AppError::Database { source }
}

#[cfg(test)]
mod tests {
    use tribal_domain::ProviderConnectionName;

    use super::*;

    #[test]
    fn test_genesis_base_url_refuses_a_provider_with_no_local_endpoint() {
        let mut config = TribalConfig::default();
        config.provider_connections.insert(
            ProviderConnectionName::parse("platform_default").unwrap(),
            tribal_config::ProviderConnectionConfig::Platform {},
        );
        config.init.embedding.connection =
            ProviderConnectionName::parse("platform_default").unwrap();
        let err = genesis_base_url(&config).expect_err("platform has no local endpoint");
        assert!(
            matches!(err, AppError::ProviderSetup { .. }),
            "expected an honest ProviderSetup rejection, got {err:?}",
        );
    }
}
