//! Outcome constructors and probe for the `valid_token_exists` check.
//!
//! Resolution order under `http` / `sse`: `--token` → `TRIBAL_AUTH_TOKEN`
//! → `credentials.json`.  If every source is empty, the check falls
//! through to an aggregate `any_active` lookup against the database.
//! Under `stdio`, only `--token` is consulted; an absent override
//! yields `Skip`.

use std::sync::Arc;

use chrono::Utc;
use sqlx::PgPool;
use tribal_config::{Auth, CredentialsReadError, ENV_AUTH_TOKEN, TransportKind, read_credentials};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository, PgPrincipalRepository};
use tribal_mcp::{AuthError, Authenticator};

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation, TokenFailureReason, TokenTransport},
};

impl CheckOutcome {
    pub(in crate::commands::check) fn token_skipped_stdio() -> Self {
        Self::Skip {
            detail: CheckDetail::TokenSkippedStdio,
        }
    }

    pub(in crate::commands::check) fn token_verified(transport: TokenTransport) -> Self {
        Self::Pass {
            detail: CheckDetail::TokenVerified { transport },
        }
    }

    pub(in crate::commands::check) fn token_verification_failed(
        transport: TokenTransport,
        reason: TokenFailureReason,
    ) -> Self {
        let remediation = match &reason {
            TokenFailureReason::DatabaseUnavailable { .. } => CheckRemediation::CheckPgIsready,
            TokenFailureReason::Invalid
            | TokenFailureReason::Revoked
            | TokenFailureReason::Expired
            | TokenFailureReason::PrincipalMissing => CheckRemediation::RunTribalTokenCreate,
        };
        Self::Fail {
            detail: CheckDetail::TokenVerificationFailed { transport, reason },
            remediation,
        }
    }

    pub(in crate::commands::check) fn token_aggregate_warn() -> Self {
        Self::Warn {
            detail: CheckDetail::TokenAggregateWarn,
            remediation: CheckRemediation::RunTribalTokenCreate,
        }
    }

    pub(in crate::commands::check) fn no_active_tokens() -> Self {
        Self::Fail {
            detail: CheckDetail::NoActiveTokens,
            remediation: CheckRemediation::RunTribalTokenCreate,
        }
    }

    pub(in crate::commands::check) fn token_aggregate_query_failed(error: String) -> Self {
        Self::Fail {
            detail: CheckDetail::TokenAggregateQueryFailed { error },
            remediation: CheckRemediation::ConsultUnderlyingError,
        }
    }

    pub(in crate::commands::check) fn credentials_unreadable(error: String) -> Self {
        Self::Fail {
            detail: CheckDetail::CredentialsUnreadable { error },
            remediation: CheckRemediation::RerunBootstrap,
        }
    }
}

/// Runs the transport-aware token check against the parsed config and
/// pool on `state`.
///
/// Stdio without `--token` resolves without touching the pool — mirrors
/// `require_token_resolution`'s preflight, which is why that branch
/// returns `Run` even when the pool is absent.  Every other path
/// guarantees `state.pool` is populated by the time it runs.
pub(in crate::commands::check) async fn act(state: &mut CheckState) -> CheckOutcome {
    let config = state
        .config
        .as_ref()
        .expect("preflight ensures state.config is populated");
    let token_override = state.token_override.as_deref();

    match config.server.transport {
        TransportKind::Stdio => match token_override {
            None => CheckOutcome::token_skipped_stdio(),
            Some(token) => {
                let pool = state
                    .pool
                    .as_ref()
                    .expect("preflight ensures state.pool is populated under stdio + --token");
                verify_against(pool, token, TokenTransport::Stdio).await
            }
        },
        TransportKind::Http | TransportKind::Sse => {
            let pool = state
                .pool
                .as_ref()
                .expect("preflight ensures state.pool is populated under network transport");
            network_path(pool, token_override).await
        }
    }
}

async fn network_path(pool: &PgPool, token_override: Option<&str>) -> CheckOutcome {
    if let Some(token) = token_override {
        return verify_against(pool, token, TokenTransport::Http).await;
    }
    if let Ok(token) = std::env::var(ENV_AUTH_TOKEN)
        && !token.is_empty()
    {
        return verify_against(pool, &token, TokenTransport::Http).await;
    }
    match read_credentials() {
        Ok(loaded) => match loaded.credentials.auth {
            Auth::Bearer { token } => {
                verify_against(pool, token.as_str(), TokenTransport::Http).await
            }
        },
        Err(CredentialsReadError::NotFound) => check_aggregate(pool).await,
        Err(
            err @ (CredentialsReadError::Path(_)
            | CredentialsReadError::Read { .. }
            | CredentialsReadError::Malformed { .. }
            | CredentialsReadError::UnsupportedSchema { .. }),
        ) => CheckOutcome::credentials_unreadable(err.to_string()),
    }
}

async fn verify_against(pool: &PgPool, token: &str, transport: TokenTransport) -> CheckOutcome {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(err) => {
            return CheckOutcome::token_verification_failed(
                transport,
                TokenFailureReason::DatabaseUnavailable {
                    context: err.to_string(),
                },
            );
        }
    };
    let authenticator = Authenticator::new(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
    );
    let Err(err) = authenticator.verify_token(&mut conn, token).await else {
        return CheckOutcome::token_verified(transport);
    };
    outcome_for_auth_error(err, transport)
}

async fn check_aggregate(pool: &PgPool) -> CheckOutcome {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(err) => return CheckOutcome::token_aggregate_query_failed(err.to_string()),
    };
    match PgAuthTokenRepository
        .any_active(&mut conn, Utc::now())
        .await
    {
        Ok(true) => CheckOutcome::token_aggregate_warn(),
        Ok(false) => CheckOutcome::no_active_tokens(),
        Err(err) => CheckOutcome::token_aggregate_query_failed(err.to_string()),
    }
}

/// Maps an [`AuthError`] from [`Authenticator::verify_token`] to the
/// matching [`CheckOutcome`] under `transport`.
///
/// The match is exhaustive over every `AuthError` variant; adding a
/// new variant is a compile-time obligation here, never a silent
/// reclassification.  `InsufficientScope` is the one variant that
/// resolves to a *successful* outcome — scope checks belong to per-tool
/// calls, not to the existence check this row enforces.
/// `LocalPrincipalMissing` folds into `PrincipalMissing` so both
/// "valid token, principal vanished" cases render the same way.
fn outcome_for_auth_error(err: AuthError, transport: TokenTransport) -> CheckOutcome {
    match err {
        AuthError::InvalidToken { .. } => {
            CheckOutcome::token_verification_failed(transport, TokenFailureReason::Invalid)
        }
        AuthError::TokenRevoked { .. } => {
            CheckOutcome::token_verification_failed(transport, TokenFailureReason::Revoked)
        }
        AuthError::TokenExpired { .. } => {
            CheckOutcome::token_verification_failed(transport, TokenFailureReason::Expired)
        }
        AuthError::PrincipalNotFound { .. } | AuthError::LocalPrincipalMissing { .. } => {
            CheckOutcome::token_verification_failed(transport, TokenFailureReason::PrincipalMissing)
        }
        AuthError::DatabaseUnavailable { context, .. } => CheckOutcome::token_verification_failed(
            transport,
            TokenFailureReason::DatabaseUnavailable { context },
        ),
        AuthError::InsufficientScope { .. } => CheckOutcome::token_verified(transport),
    }
}

#[cfg(test)]
mod tests {
    use tribal_domain::{PrincipalId, Scope};

    use super::*;

    #[test]
    fn test_token_skipped_stdio_is_skip() {
        assert!(matches!(
            &CheckOutcome::token_skipped_stdio(),
            CheckOutcome::Skip {
                detail: CheckDetail::TokenSkippedStdio,
            },
        ));
    }

    #[test]
    fn test_token_verified_is_pass_for_each_transport() {
        for transport in [TokenTransport::Stdio, TokenTransport::Http] {
            let outcome = CheckOutcome::token_verified(transport);
            assert!(matches!(
                &outcome,
                CheckOutcome::Pass {
                    detail: CheckDetail::TokenVerified { transport: t },
                } if *t == transport,
            ));
        }
    }

    #[test]
    fn test_token_verification_failed_token_shape_routes_to_run_token_create() {
        for reason in [
            TokenFailureReason::Invalid,
            TokenFailureReason::Revoked,
            TokenFailureReason::Expired,
            TokenFailureReason::PrincipalMissing,
        ] {
            let outcome =
                CheckOutcome::token_verification_failed(TokenTransport::Http, reason.clone());
            assert!(
                matches!(
                    &outcome,
                    CheckOutcome::Fail {
                        remediation: CheckRemediation::RunTribalTokenCreate,
                        ..
                    },
                ),
                "reason {reason:?} should route to RunTribalTokenCreate, got {outcome:?}",
            );
        }
    }

    #[test]
    fn test_token_verification_failed_database_unavailable_routes_to_pg_isready() {
        let outcome = CheckOutcome::token_verification_failed(
            TokenTransport::Http,
            TokenFailureReason::DatabaseUnavailable {
                context: "pool exhausted".into(),
            },
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenVerificationFailed {
                    reason: TokenFailureReason::DatabaseUnavailable { context },
                    ..
                },
                remediation: CheckRemediation::CheckPgIsready,
            } if context == "pool exhausted",
        ));
    }

    #[test]
    fn test_token_aggregate_warn_is_warn() {
        assert!(matches!(
            &CheckOutcome::token_aggregate_warn(),
            CheckOutcome::Warn {
                detail: CheckDetail::TokenAggregateWarn,
                remediation: CheckRemediation::RunTribalTokenCreate,
            },
        ));
    }

    #[test]
    fn test_no_active_tokens_is_fail() {
        assert!(matches!(
            &CheckOutcome::no_active_tokens(),
            CheckOutcome::Fail {
                detail: CheckDetail::NoActiveTokens,
                remediation: CheckRemediation::RunTribalTokenCreate,
            },
        ));
    }

    #[test]
    fn test_credentials_unreadable_is_fail_with_rerun_bootstrap() {
        let outcome = CheckOutcome::credentials_unreadable("malformed JSON".into());
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::CredentialsUnreadable { error },
                remediation: CheckRemediation::RerunBootstrap,
            } if error == "malformed JSON",
        ));
    }

    #[test]
    fn test_token_aggregate_query_failed_routes_to_consult_underlying_error() {
        let outcome = CheckOutcome::token_aggregate_query_failed("permission denied".into());
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenAggregateQueryFailed { error },
                remediation: CheckRemediation::ConsultUnderlyingError,
            } if error == "permission denied",
        ));
    }

    #[test]
    fn test_outcome_for_auth_error_invalid_token_is_fail_invalid() {
        let outcome = outcome_for_auth_error(
            AuthError::InvalidToken {
                token_hash: "h".into(),
            },
            TokenTransport::Http,
        );
        assert!(matches!(
            outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenVerificationFailed {
                    reason: TokenFailureReason::Invalid,
                    ..
                },
                ..
            },
        ));
    }

    #[test]
    fn test_outcome_for_auth_error_token_revoked_is_fail_revoked() {
        let outcome = outcome_for_auth_error(
            AuthError::TokenRevoked {
                token_hash: "h".into(),
            },
            TokenTransport::Http,
        );
        assert!(matches!(
            outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenVerificationFailed {
                    reason: TokenFailureReason::Revoked,
                    ..
                },
                ..
            },
        ));
    }

    #[test]
    fn test_outcome_for_auth_error_token_expired_is_fail_expired() {
        let outcome = outcome_for_auth_error(
            AuthError::TokenExpired {
                token_hash: "h".into(),
            },
            TokenTransport::Http,
        );
        assert!(matches!(
            outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenVerificationFailed {
                    reason: TokenFailureReason::Expired,
                    ..
                },
                ..
            },
        ));
    }

    #[test]
    fn test_outcome_for_auth_error_principal_not_found_is_fail_principal_missing() {
        let pid: PrincipalId = "prin_550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid prin");
        let outcome = outcome_for_auth_error(
            AuthError::PrincipalNotFound { principal_id: pid },
            TokenTransport::Http,
        );
        assert!(matches!(
            outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenVerificationFailed {
                    reason: TokenFailureReason::PrincipalMissing,
                    ..
                },
                ..
            },
        ));
    }

    #[test]
    fn test_outcome_for_auth_error_local_principal_missing_folds_into_principal_missing() {
        let outcome = outcome_for_auth_error(
            AuthError::LocalPrincipalMissing {
                principal_key: "principal:local".into(),
            },
            TokenTransport::Stdio,
        );
        assert!(matches!(
            outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenVerificationFailed {
                    reason: TokenFailureReason::PrincipalMissing,
                    transport: TokenTransport::Stdio,
                },
                ..
            },
        ));
    }

    #[test]
    fn test_outcome_for_auth_error_database_unavailable_preserves_context() {
        let outcome = outcome_for_auth_error(
            AuthError::DatabaseUnavailable {
                context: "boom".into(),
                source: Box::new(std::io::Error::other("io")),
            },
            TokenTransport::Http,
        );
        assert!(matches!(
            outcome,
            CheckOutcome::Fail {
                detail: CheckDetail::TokenVerificationFailed {
                    reason: TokenFailureReason::DatabaseUnavailable { context },
                    ..
                },
                ..
            } if context == "boom",
        ));
    }

    #[test]
    fn test_outcome_for_auth_error_insufficient_scope_is_pass() {
        let outcome = outcome_for_auth_error(
            AuthError::InsufficientScope {
                required_scope: Scope::parse(Scope::FULL_ACCESS_WRITE).expect("valid scope"),
                granted_scopes: vec![],
            },
            TokenTransport::Http,
        );
        assert!(matches!(
            outcome,
            CheckOutcome::Pass {
                detail: CheckDetail::TokenVerified {
                    transport: TokenTransport::Http,
                },
            },
        ));
    }
}
