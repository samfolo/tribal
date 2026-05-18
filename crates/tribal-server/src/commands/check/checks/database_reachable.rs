//! Outcome constructors and probe for the `database_reachable` check.
//!
//! Probes the configured database by opening a single connection.  The
//! outcome carries no URL information — the user's configured URL lives
//! in their config file, and the sqlx error on failure carries enough
//! diagnostic context (host, error kind) without including credentials.

use sqlx::{Connection, PgConnection};
use tribal_config::DatabaseConfig;

use super::types::{CheckDetail, CheckName, CheckOutcome, CheckRemediation};

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

/// Opens a single connection to the configured database and reports the
/// outcome.  The connection drops at function exit.
pub(in crate::commands::check) async fn run(database_config: &DatabaseConfig) -> CheckOutcome {
    let Err(err) = PgConnection::connect(&database_config.url).await else {
        return CheckOutcome::database_reachable();
    };
    CheckOutcome::database_unreachable(err.to_string())
}

#[cfg(test)]
mod tests {
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
    /// in the rendered message.  If this test ever fails, switch the
    /// outcome detail to a strictly redacted form before merging.
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
