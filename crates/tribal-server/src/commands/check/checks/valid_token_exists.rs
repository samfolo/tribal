//! Outcome constructors and probe for the `valid_token_exists` check.
//!
//! Resolution order under `http` / `sse`: `--token` → `TRIBAL_AUTH_TOKEN`
//! → `credentials.json`.  If every source is empty, the outcome turns on
//! deployment topology: a loopback surface skips (network clients
//! authenticate via OAuth), while a routable surface warns (open
//! registration is refused, so an absent static token leaves clients no
//! authentication path).  Under `stdio`, only `--token` is consulted; an
//! absent override yields `Skip`.

use std::sync::Arc;

use sqlx::PgPool;
use tribal_auth::{AuthError, Authenticator};
use tribal_config::{
    Auth, CredentialsReadError, ENV_AUTH_TOKEN, TransportKind, oauth_surface_is_routable,
    read_credentials,
};
use tribal_db::{PgAuthTokenRepository, PgPrincipalRepository};

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation, TokenFailureReason, TokenTransport},
};
use crate::startup::{expected_token_audience, resolve_oauth_runtime};

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

    pub(in crate::commands::check) fn token_skipped_loopback_oauth() -> Self {
        Self::Skip {
            detail: CheckDetail::TokenSkippedLoopbackOauth,
        }
    }

    pub(in crate::commands::check) fn token_missing_routable() -> Self {
        Self::Warn {
            detail: CheckDetail::TokenMissingRoutable,
            remediation: CheckRemediation::RunTribalTokenCreate,
        }
    }

    pub(in crate::commands::check) fn credentials_unreadable(
        error: String,
        remediation: CheckRemediation,
    ) -> Self {
        Self::Fail {
            detail: CheckDetail::CredentialsUnreadable { error },
            remediation,
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
                // Stdio does not bind a bearer audience.
                verify_against(pool, token, TokenTransport::Stdio, None).await
            }
        },
        TransportKind::Http | TransportKind::Sse => {
            let pool = state
                .pool
                .as_ref()
                .expect("preflight ensures state.pool is populated under network transport");
            // Match the live network middleware, which binds the token
            // audience to the canonical resource: a token minted for a
            // different resource must fail this check exactly as it would
            // at `/mcp`. A resolve failure leaves the audience unbound
            // rather than failing the existence check outright.
            let expected_audience = resolve_oauth_runtime(config)
                .ok()
                .and_then(|runtime| expected_token_audience(config.server.transport, &runtime));
            let oauth_surface_routable = oauth_surface_is_routable(config);
            network_path(
                pool,
                token_override,
                expected_audience,
                oauth_surface_routable,
            )
            .await
        }
    }
}

async fn network_path(
    pool: &PgPool,
    token_override: Option<&str>,
    expected_audience: Option<String>,
    oauth_surface_routable: bool,
) -> CheckOutcome {
    if let Some(token) = token_override {
        return verify_against(pool, token, TokenTransport::Http, expected_audience).await;
    }
    if let Ok(token) = std::env::var(ENV_AUTH_TOKEN)
        && !token.trim().is_empty()
    {
        return verify_against(pool, token.trim(), TokenTransport::Http, expected_audience).await;
    }
    match read_credentials() {
        Ok(loaded) => match loaded.credentials.auth {
            Auth::Bearer { token } => {
                verify_against(
                    pool,
                    token.as_str(),
                    TokenTransport::Http,
                    expected_audience,
                )
                .await
            }
        },
        // No static token resolves. On a loopback surface that is fine:
        // network clients authenticate via OAuth. On a routable surface
        // open registration is refused, so the absent token is a gap.
        Err(CredentialsReadError::NotFound) => {
            if oauth_surface_routable {
                CheckOutcome::token_missing_routable()
            } else {
                CheckOutcome::token_skipped_loopback_oauth()
            }
        }
        Err(
            err @ (CredentialsReadError::Malformed { .. }
            | CredentialsReadError::UnsupportedSchema { .. }),
        ) => {
            CheckOutcome::credentials_unreadable(err.to_string(), CheckRemediation::RerunBootstrap)
        }
        Err(err @ (CredentialsReadError::Path(_) | CredentialsReadError::Read { .. })) => {
            CheckOutcome::credentials_unreadable(
                err.to_string(),
                CheckRemediation::ConsultUnderlyingError,
            )
        }
    }
}

async fn verify_against(
    pool: &PgPool,
    token: &str,
    transport: TokenTransport,
    expected_audience: Option<String>,
) -> CheckOutcome {
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
    let authenticator = Authenticator::with_audience(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
        expected_audience,
    );
    let Err(err) = authenticator.verify_token(&mut conn, token).await else {
        return CheckOutcome::token_verified(transport);
    };
    outcome_for_auth_error(err, transport)
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
        AuthError::InvalidToken { .. } | AuthError::AudienceMismatch { .. } => {
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
    fn test_token_skipped_loopback_oauth_is_skip() {
        assert!(matches!(
            &CheckOutcome::token_skipped_loopback_oauth(),
            CheckOutcome::Skip {
                detail: CheckDetail::TokenSkippedLoopbackOauth,
            },
        ));
    }

    #[test]
    fn test_token_missing_routable_is_warn_routing_to_token_create() {
        assert!(matches!(
            &CheckOutcome::token_missing_routable(),
            CheckOutcome::Warn {
                detail: CheckDetail::TokenMissingRoutable,
                remediation: CheckRemediation::RunTribalTokenCreate,
            },
        ));
    }

    #[test]
    fn test_credentials_unreadable_threads_supplied_remediation() {
        for remediation in [
            CheckRemediation::RerunBootstrap,
            CheckRemediation::ConsultUnderlyingError,
        ] {
            let outcome =
                CheckOutcome::credentials_unreadable("malformed JSON".into(), remediation.clone());
            assert!(
                matches!(
                    &outcome,
                    CheckOutcome::Fail {
                        detail: CheckDetail::CredentialsUnreadable { error },
                        remediation: r,
                    } if error == "malformed JSON" && *r == remediation,
                ),
                "expected {remediation:?} to be threaded through, got {outcome:?}",
            );
        }
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
