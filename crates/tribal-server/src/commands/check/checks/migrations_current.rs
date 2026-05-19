//! Outcome constructors and action for the `migrations_current` check.

use tribal_db::{MIGRATOR, MigrationHeadStatus, MigrationRepository, PgMigrationRepository};

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};

impl CheckOutcome {
    pub(in crate::commands::check) fn migrations_match() -> Self {
        Self::Pass {
            detail: CheckDetail::MigrationsMatch,
        }
    }

    pub(in crate::commands::check) fn migrations_behind(expected: i64, found: i64) -> Self {
        Self::Fail {
            detail: CheckDetail::MigrationsBehind { expected, found },
            remediation: CheckRemediation::RunTribalSetup,
        }
    }

    pub(in crate::commands::check) fn migrations_ahead(expected: i64, found: i64) -> Self {
        Self::Fail {
            detail: CheckDetail::MigrationsAhead { expected, found },
            remediation: CheckRemediation::UpgradeBinary,
        }
    }

    pub(in crate::commands::check) fn migrations_table_missing() -> Self {
        Self::Fail {
            detail: CheckDetail::MigrationsTableMissing,
            remediation: CheckRemediation::RunTribalSetup,
        }
    }

    pub(in crate::commands::check) fn migrations_query_failed(error: String) -> Self {
        Self::Fail {
            detail: CheckDetail::MigrationsQueryFailed { error },
            remediation: CheckRemediation::ConsultUnderlyingError,
        }
    }
}

/// Compares the database's recorded head migration version against the
/// binary's compile-time head.
pub(in crate::commands::check) async fn act(state: &mut CheckState) -> CheckOutcome {
    let pool = state
        .pool
        .as_ref()
        .expect("preflight ensures state.pool is populated");
    let expected = MIGRATOR
        .iter()
        .last()
        .expect("MIGRATOR contains at least one migration")
        .version;

    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(err) => return CheckOutcome::migrations_query_failed(err.to_string()),
    };

    match PgMigrationRepository
        .current_head_matches(&mut conn, expected)
        .await
    {
        Ok(MigrationHeadStatus::Matches) => CheckOutcome::migrations_match(),
        Ok(MigrationHeadStatus::Behind { expected, found }) => {
            CheckOutcome::migrations_behind(expected, found)
        }
        Ok(MigrationHeadStatus::Ahead { expected, found }) => {
            CheckOutcome::migrations_ahead(expected, found)
        }
        Ok(MigrationHeadStatus::MissingTable) => CheckOutcome::migrations_table_missing(),
        Err(err) => CheckOutcome::migrations_query_failed(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrations_match_is_pass() {
        assert!(matches!(
            &CheckOutcome::migrations_match(),
            CheckOutcome::Pass {
                detail: CheckDetail::MigrationsMatch,
            },
        ));
    }

    #[test]
    fn test_migrations_behind_carries_versions_and_setup_remediation() {
        assert!(matches!(
            &CheckOutcome::migrations_behind(42, 30),
            CheckOutcome::Fail {
                detail: CheckDetail::MigrationsBehind {
                    expected: 42,
                    found: 30,
                },
                remediation: CheckRemediation::RunTribalSetup,
            },
        ));
    }

    #[test]
    fn test_migrations_ahead_carries_versions_and_upgrade_remediation() {
        assert!(matches!(
            &CheckOutcome::migrations_ahead(30, 42),
            CheckOutcome::Fail {
                detail: CheckDetail::MigrationsAhead {
                    expected: 30,
                    found: 42,
                },
                remediation: CheckRemediation::UpgradeBinary,
            },
        ));
    }

    #[test]
    fn test_migrations_table_missing_recommends_setup() {
        assert!(matches!(
            &CheckOutcome::migrations_table_missing(),
            CheckOutcome::Fail {
                detail: CheckDetail::MigrationsTableMissing,
                remediation: CheckRemediation::RunTribalSetup,
            },
        ));
    }

    #[test]
    fn test_migrations_query_failed_routes_to_consult_underlying_error() {
        let outcome = CheckOutcome::migrations_query_failed("permission denied".into());
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::MigrationsQueryFailed { error },
                remediation: CheckRemediation::ConsultUnderlyingError,
            } if error == "permission denied",
        ));
    }
}
