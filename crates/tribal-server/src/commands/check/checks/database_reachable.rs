//! Outcome constructors and probe for the `database_reachable` check.
//!
//! Builds the shared pool the rest of the database-dependent checks
//! reuse.  The pool is returned alongside the outcome so the
//! orchestrator can thread it into [`CheckContext`].  No URL crosses
//! the wire — the configured URL lives in the user's config file and
//! the sqlx error provides diagnostic context without leaking
//! credentials.

use sqlx::PgPool;
use tribal_config::DatabaseConfig;
use tribal_db::create_pool;

use super::types::{CheckDetail, CheckName, CheckOutcome, CheckRemediation};
use crate::commands::common::{COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS};

/// Pool-name tag passed to [`create_pool`] for tracing.
const POOL_NAME: &str = "check";

impl CheckOutcome {
    pub(in crate::commands::check) fn database_reachable() -> Self {
        Self::Pass {
            name: CheckName::DatabaseReachable,
            detail: CheckDetail::DatabaseReachable,
        }
    }

    pub(in crate::commands::check) fn database_unreachable(error: String) -> Self {
        Self::Fail {
            name: CheckName::DatabaseReachable,
            detail: CheckDetail::DatabaseUnreachable { error },
            remediation: Some(CheckRemediation::CheckPgIsready),
        }
    }
}

/// Builds the shared `tribal check` pool and reports the outcome.  On
/// success returns the pool for downstream database-dependent checks;
/// on failure returns `None`.
pub(in crate::commands::check) async fn run(
    database_config: &DatabaseConfig,
) -> (CheckOutcome, Option<PgPool>) {
    match create_pool(
        database_config,
        POOL_NAME,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    {
        Ok(pool) => (CheckOutcome::database_reachable(), Some(pool)),
        Err(err) => (CheckOutcome::database_unreachable(err.to_string()), None),
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{Connection, PgConnection};

    use super::*;

    #[test]
    fn test_database_reachable_outcome_is_pass() {
        assert!(matches!(
            &CheckOutcome::database_reachable(),
            CheckOutcome::Pass {
                name: CheckName::DatabaseReachable,
                detail: CheckDetail::DatabaseReachable,
            },
        ));
    }

    #[test]
    fn test_database_unreachable_outcome_carries_error_and_remediation() {
        let outcome = CheckOutcome::database_unreachable("connection refused".into());
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                name: CheckName::DatabaseReachable,
                detail: CheckDetail::DatabaseUnreachable { error },
                remediation: Some(CheckRemediation::CheckPgIsready),
            } if error == "connection refused",
        ));
    }

    /// Asserts that `sqlx::Error::to_string()` for a failed connection
    /// against a URL carrying a password never includes that password
    /// in the rendered message.  `create_pool` wraps this in
    /// `DbError::QueryFailed` via `{source}` interpolation, so the same
    /// guarantee transitively covers our error path.  If this test ever
    /// fails, switch the outcome detail to a strictly redacted form
    /// before merging.
    #[tokio::test]
    async fn test_sqlx_error_does_not_leak_password_in_connection_string() {
        let url =
            "postgres://user:hunter2-very-secret@no-such-host-for-tribal-check.invalid:5432/db";
        let Err(error) = PgConnection::connect(url).await else {
            panic!("expected connection failure against a non-existent host");
        };
        let rendered = error.to_string();
        assert!(
            !rendered.contains("hunter2-very-secret"),
            "sqlx leaked the password into its error message: {rendered}",
        );
    }
}
