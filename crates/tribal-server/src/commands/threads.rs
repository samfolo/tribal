//! `tribal threads` operator commands.
//!
//! The thread tables are durable by default and are not a supported SQL
//! surface, so the prune command is the one sanctioned way to reclaim
//! their storage: explicit criteria, terminal threads only, refusing any
//! candidate whose subtree still holds a live thread unless the cascade
//! is explicit. `--dry-run` reports what a pass would collect without
//! deleting anything.

use std::io::{self, Write};

use chrono::{Duration, Utc};
use sqlx::PgPool;
use tribal_config::TribalConfig;
use tribal_db::{
    AgentThreadRepository, DbError, PgAgentThreadRepository, ThreadPruneCriteria,
    ThreadPruneOutcome, create_pool,
};

use crate::{
    cli::ThreadsPruneArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
        prepare_config,
    },
    error::AppError,
};

const POOL_NAME: &str = "threads";

/// Runs `tribal threads prune`.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, the database connection, or
/// the prune transaction fails.
pub(crate) fn prune(config_path: &str, args: ThreadsPruneArgs) -> Result<(), AppError> {
    let criteria = ThreadPruneCriteria {
        completed_before: Utc::now() - Duration::days(i64::from(args.older_than_days)),
        stage: args.stage,
        cascade: args.cascade,
    };
    let dry_run = args.dry_run;
    let config = prepare_config(
        config_path,
        args.database.into_cli_overrides(),
        &DATABASE_COMMAND_DEFAULTS,
    )?;
    runtime()?.block_on(prune_async(&config, &criteria, dry_run))
}

async fn prune_async(
    config: &TribalConfig,
    criteria: &ThreadPruneCriteria,
    dry_run: bool,
) -> Result<(), AppError> {
    let pool = command_pool(config).await?;
    let mut tx = begin(&pool).await?;

    let refused = PgAgentThreadRepository
        .count_refused_prune_roots(&mut tx, criteria)
        .await
        .map_err(|source| AppError::Database { source })?;
    let pruned = PgAgentThreadRepository
        .prune_threads(&mut tx, criteria)
        .await
        .map_err(|source| AppError::Database { source })?;
    let outcome = ThreadPruneOutcome { pruned, refused };

    if dry_run {
        // The dry run derives its counts from the real pass, then rolls
        // the whole transaction back.
        drop(tx);
        report(&outcome, true)
    } else {
        commit(tx).await?;
        report(&outcome, false)
    }
}

fn report(outcome: &ThreadPruneOutcome, dry_run: bool) -> Result<(), AppError> {
    let verb = if dry_run { "would prune" } else { "pruned" };
    let mut out = io::stdout().lock();
    writeln!(
        out,
        "{verb} {} thread(s) with their records",
        outcome.pruned
    )
    .map_err(|source| AppError::Io {
        context: "writing threads prune output".to_owned(),
        source,
    })?;
    if outcome.refused > 0 {
        writeln!(
            out,
            "refused {} candidate(s) with descendants — resolve or cancel live ones, or pass \
             --cascade to collect terminal subtrees",
            outcome.refused,
        )
        .map_err(|source| AppError::Io {
            context: "writing threads prune output".to_owned(),
            source,
        })?;
    }
    Ok(())
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
            context: "beginning the threads prune transaction".to_owned(),
            source,
        },
    })
}

async fn commit(tx: sqlx::Transaction<'static, sqlx::Postgres>) -> Result<(), AppError> {
    tx.commit().await.map_err(|source| AppError::Database {
        source: DbError::QueryFailed {
            context: "committing the threads prune".to_owned(),
            source,
        },
    })
}
