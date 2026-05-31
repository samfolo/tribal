//! `tribal reindex` operator commands.
//!
//! Each command opens a command pool and drives the shared
//! [`tribal_worker`] reindex services directly: `run` creates a queued run
//! the running server's worker then drains, `cancel` aborts the live run, and
//! `prune` reclaims superseded profiles. The create path builds the target
//! provider against a throwaway registry and, for a real run, probes its drift
//! signal, so it needs no active profile.

use std::io::{self, Write};

use sqlx::PgPool;
use tribal_config::TribalConfig;
use tribal_db::{DbError, create_pool};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, PrincipalId};
use tribal_inference::ProviderRegistry;
use tribal_worker::{
    ReindexCancelOutcome, ReindexRunOutcome, ReindexRunRequest, ReindexRunStatus,
    drop_superseded_indexes, reindex_cancel, reindex_prune, reindex_run,
};

use crate::{
    cli::{DatabaseArgs, ReindexRunArgs},
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
        find_or_create_principal, prepare_config,
    },
    error::AppError,
};

const POOL_NAME: &str = "reindex";

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

/// Runs `tribal reindex run`.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, the database connection, or the
/// reindex create fails.
pub(crate) fn run(config_path: &str, args: ReindexRunArgs) -> Result<(), AppError> {
    let request = ReindexRunRequest {
        provider: args.provider.as_str().to_owned(),
        model: args.model,
        dimensions: args.dimensions,
        base_url: args.base_url,
        dry_run: args.dry_run,
    };
    let cli_overrides = args.database.into_cli_overrides();
    let config = prepare_config(config_path, cli_overrides, &DATABASE_COMMAND_DEFAULTS)?;
    runtime()?.block_on(run_async(&config, request))
}

async fn run_async(config: &TribalConfig, request: ReindexRunRequest) -> Result<(), AppError> {
    let pool = command_pool(config).await?;

    // The create path registers the target endpoint dynamically and probes it
    // once, so a throwaway registry with no active-profile entry suffices.
    let registry = ProviderRegistry::new(std::iter::empty())
        .map_err(|source| AppError::ProviderRegistry { source })?;

    // A dry run creates no row and so needs no initiating principal; resolve it
    // only on the create path, keeping the estimate genuinely read-only.
    let principal_id = if request.dry_run {
        PrincipalId::new()
    } else {
        let mut conn = pool.acquire().await.map_err(|err| {
            AppError::pool_acquire(POOL_NAME, "acquiring reindex connection", err)
        })?;
        let principal = find_or_create_principal(&mut conn, LOCAL_PRINCIPAL_KEY).await?;
        principal.id()
    };

    let outcome = reindex_run(
        &pool,
        &registry,
        &config.credentials,
        &request,
        principal_id,
    )
    .await
    .map_err(|source| AppError::Reindex { source })?;

    render_run(&mut io::stdout().lock(), &outcome)
}

fn render_run(out: &mut dyn Write, outcome: &ReindexRunOutcome) -> Result<(), AppError> {
    let target = format!(
        "{}/{} ({}d) at {}",
        outcome.provider, outcome.model, outcome.dimensions, outcome.normalised_base_url,
    );
    let estimate = format!(
        "{} item(s) and {} tag(s) to embed",
        outcome.estimated_items, outcome.estimated_tags,
    );
    let line = match (outcome.status, &outcome.run_id) {
        (ReindexRunStatus::Plan, _) => {
            format!("plan: reindex {target} would embed {estimate}")
        }
        (ReindexRunStatus::Created, Some(id)) => {
            format!("created reindex run {id} for {target}: {estimate}")
        }
        (ReindexRunStatus::AlreadyLive, Some(id)) => {
            format!("already live: reindex run {id} is resuming {target}: {estimate}")
        }
        (ReindexRunStatus::Unchanged, _) => {
            format!("unchanged: the corpus already uses {target}")
        }
        (ReindexRunStatus::LockContended, _) => {
            "lock contended: another reindex is starting; retry shortly".to_owned()
        }
        (ReindexRunStatus::Created | ReindexRunStatus::AlreadyLive, None) => {
            format!("{}: {target}: {estimate}", outcome.status.label())
        }
    };
    writeln!(out, "{line}").map_err(|source| AppError::Io {
        context: "writing reindex output".to_owned(),
        source,
    })
}

// ---------------------------------------------------------------------------
// cancel
// ---------------------------------------------------------------------------

/// Runs `tribal reindex cancel`.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, the database connection, or the
/// cancel transaction fails.
pub(crate) fn cancel(config_path: &str, args: DatabaseArgs) -> Result<(), AppError> {
    let config = prepare_config(
        config_path,
        args.into_cli_overrides(),
        &DATABASE_COMMAND_DEFAULTS,
    )?;
    runtime()?.block_on(cancel_async(&config))
}

async fn cancel_async(config: &TribalConfig) -> Result<(), AppError> {
    let pool = command_pool(config).await?;
    let mut tx = begin(&pool).await?;
    let outcome = reindex_cancel(&mut tx)
        .await
        .map_err(|source| AppError::Database { source })?;
    commit(tx, "reindex cancel").await?;

    let line = match outcome {
        ReindexCancelOutcome::Cancelled(id) => format!("cancelled reindex run {id}"),
        ReindexCancelOutcome::NoLiveRun => "no live reindex run to cancel".to_owned(),
    };
    writeln!(io::stdout().lock(), "{line}").map_err(|source| AppError::Io {
        context: "writing reindex cancel output".to_owned(),
        source,
    })
}

// ---------------------------------------------------------------------------
// prune
// ---------------------------------------------------------------------------

/// Runs `tribal reindex prune`.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, the database connection, or the
/// prune transaction fails.
pub(crate) fn prune(config_path: &str, args: DatabaseArgs) -> Result<(), AppError> {
    let config = prepare_config(
        config_path,
        args.into_cli_overrides(),
        &DATABASE_COMMAND_DEFAULTS,
    )?;
    runtime()?.block_on(prune_async(&config))
}

async fn prune_async(config: &TribalConfig) -> Result<(), AppError> {
    let pool = command_pool(config).await?;
    let mut tx = begin(&pool).await?;
    let outcome = reindex_prune(&mut tx)
        .await
        .map_err(|source| AppError::Database { source })?;
    commit(tx, "reindex prune").await?;

    // Reclaim the superseded profiles' partial indexes outside the
    // transaction; the deletes have committed, so this is best-effort.
    if !outcome.superseded_epochs.is_empty()
        && let Ok(mut conn) = pool.acquire().await
    {
        drop_superseded_indexes(&mut conn, &outcome.superseded_epochs).await;
    }

    writeln!(
        io::stdout().lock(),
        "pruned {} profile(s): deleted {} embedding(s) and {} tag embedding(s)",
        outcome.profiles_superseded,
        outcome.embeddings_deleted,
        outcome.tag_embeddings_deleted,
    )
    .map_err(|source| AppError::Io {
        context: "writing reindex prune output".to_owned(),
        source,
    })
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn runtime() -> Result<tokio::runtime::Runtime, AppError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })
}

async fn command_pool(config: &TribalConfig) -> Result<PgPool, AppError> {
    create_pool(
        &config.database,
        POOL_NAME,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })
}

async fn begin(pool: &PgPool) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, AppError> {
    pool.begin().await.map_err(|source| AppError::Database {
        source: DbError::QueryFailed {
            context: "beginning the reindex transaction".to_owned(),
            source,
        },
    })
}

async fn commit(
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
    context: &str,
) -> Result<(), AppError> {
    tx.commit().await.map_err(|source| AppError::Database {
        source: DbError::QueryFailed {
            context: format!("committing {context}"),
            source,
        },
    })
}

#[cfg(test)]
mod tests {
    use tribal_domain::{ProviderKind, ReindexRunId};

    use super::*;

    fn outcome(status: ReindexRunStatus, run_id: Option<ReindexRunId>) -> ReindexRunOutcome {
        ReindexRunOutcome {
            status,
            run_id,
            provider: ProviderKind::Ollama,
            model: "nomic-embed-text:v1.5".to_owned(),
            dimensions: 768,
            normalised_base_url: "http://localhost:11434".to_owned(),
            estimated_items: 3,
            estimated_tags: 1,
        }
    }

    fn render(status: ReindexRunStatus, run_id: Option<ReindexRunId>) -> String {
        let mut buf = Vec::new();
        render_run(&mut buf, &outcome(status, run_id)).expect("render");
        String::from_utf8(buf).expect("utf8")
    }

    #[test]
    fn test_render_run_covers_every_status() {
        let id = ReindexRunId::new();
        assert!(render(ReindexRunStatus::Plan, None).starts_with("plan:"));
        assert!(
            render(ReindexRunStatus::Created, Some(id))
                .contains(&format!("created reindex run {id}"))
        );
        assert!(render(ReindexRunStatus::AlreadyLive, Some(id)).starts_with("already live:"));
        assert!(render(ReindexRunStatus::Unchanged, None).starts_with("unchanged:"));
        assert!(render(ReindexRunStatus::LockContended, None).starts_with("lock contended"));
    }

    #[test]
    fn test_render_run_falls_back_for_a_created_status_without_a_run_id() {
        // The producer never pairs Created/AlreadyLive with None, but the
        // renderer degrades to the status label rather than panicking.
        assert!(render(ReindexRunStatus::Created, None).starts_with("created:"));
        assert!(render(ReindexRunStatus::AlreadyLive, None).starts_with("already_live:"));
    }
}
