//! Outcome constructors and action for the `database_reachable` check.
//!
//! Builds the shared pool the database-dependent steps later read off
//! [`CheckState`].  No URL crosses the wire — the configured URL lives
//! in the user's config file and the sqlx error provides diagnostic
//! context without leaking credentials.

use sqlx::PgPool;
use tribal_config::DatabaseConfig;
use tribal_db::create_pool;

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};
use crate::management::application::{COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS};

/// Pool-name tag passed to [`create_pool`] for tracing.
const POOL_NAME: &str = "check";

impl CheckOutcome {
    pub(in crate::management::operator_check) fn database_reachable() -> Self {
        Self::Pass {
            detail: CheckDetail::DatabaseReachable,
        }
    }

    pub(in crate::management::operator_check) fn database_unreachable(error: String) -> Self {
        Self::Fail {
            detail: CheckDetail::DatabaseUnreachable { error },
            remediation: CheckRemediation::CheckPgIsready,
        }
    }
}

/// Attempts to build the shared pool against `state.config.database` and
/// stores it on `state` on success.
pub(in crate::management::operator_check) async fn act(state: &mut CheckState) -> CheckOutcome {
    let config = state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated");
    let (outcome, pool) = probe(&config.database).await;
    state.pool = pool;
    outcome
}

async fn probe(database_config: &DatabaseConfig) -> (CheckOutcome, Option<PgPool>) {
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
                detail: CheckDetail::DatabaseUnreachable { error },
                remediation: CheckRemediation::CheckPgIsready,
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
