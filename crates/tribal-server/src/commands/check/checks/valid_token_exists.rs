//! Outcome constructors and probe for the `valid_token_exists` check.
//!
//! Resolution order under `http` / `sse`: `--token` → `TRIBAL_AUTH_TOKEN`
//! → `credentials.json`.  If every source is empty, the check falls
//! through to an aggregate `any_active` lookup against the database.
//! Under `stdio`, only `--token` is consulted; an absent override
//! yields `Skip`.

use std::sync::Arc;

use chrono::Utc;
use tribal_config::{Auth, ENV_AUTH_TOKEN, TransportKind, read_credentials};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository, PgPrincipalRepository};
use tribal_mcp::{AuthError, Authenticator};

use super::{
    context::CheckContext,
    types::{
        CheckDetail, CheckName, CheckOutcome, CheckRemediation, TokenFailureReason, TokenTransport,
    },
};

impl CheckOutcome {
    pub(in crate::commands::check) fn token_skipped_stdio() -> Self {
        Self::Skip {
            name: CheckName::ValidTokenExists,
            detail: CheckDetail::TokenSkippedStdio,
        }
    }

    pub(in crate::commands::check) fn token_verified(transport: TokenTransport) -> Self {
        Self::Pass {
            name: CheckName::ValidTokenExists,
            detail: CheckDetail::TokenVerified { transport },
        }
    }

    pub(in crate::commands::check) fn token_verification_failed(
        transport: TokenTransport,
        reason: TokenFailureReason,
    ) -> Self {
        Self::Fail {
            name: CheckName::ValidTokenExists,
            detail: CheckDetail::TokenVerificationFailed { transport, reason },
            remediation: Some(CheckRemediation::RunTribalTokenCreate),
        }
    }

    pub(in crate::commands::check) fn token_aggregate_warn() -> Self {
        Self::Warn {
            name: CheckName::ValidTokenExists,
            detail: CheckDetail::TokenAggregateWarn,
            remediation: Some(CheckRemediation::RunTribalTokenCreate),
        }
    }

    pub(in crate::commands::check) fn no_active_tokens() -> Self {
        Self::Fail {
            name: CheckName::ValidTokenExists,
            detail: CheckDetail::NoActiveTokens,
            remediation: Some(CheckRemediation::RunTribalTokenCreate),
        }
    }

    pub(in crate::commands::check) fn token_aggregate_query_failed(error: String) -> Self {
        Self::Fail {
            name: CheckName::ValidTokenExists,
            detail: CheckDetail::TokenAggregateQueryFailed { error },
            remediation: Some(CheckRemediation::CheckPgIsready),
        }
    }
}

/// Runs the transport-aware token check.
pub(in crate::commands::check) async fn run(ctx: &CheckContext) -> CheckOutcome {
    match ctx.config.server.transport {
        TransportKind::Stdio => stdio_path(ctx).await,
        TransportKind::Http | TransportKind::Sse => network_path(ctx).await,
    }
}

async fn stdio_path(ctx: &CheckContext) -> CheckOutcome {
    let Some(token) = ctx.token_override.as_deref() else {
        return CheckOutcome::token_skipped_stdio();
    };
    verify_against(ctx, token, TokenTransport::Stdio).await
}

async fn network_path(ctx: &CheckContext) -> CheckOutcome {
    if let Some(token) = ctx.token_override.as_deref() {
        return verify_against(ctx, token, TokenTransport::Http).await;
    }
    if let Ok(token) = std::env::var(ENV_AUTH_TOKEN)
        && !token.is_empty()
    {
        return verify_against(ctx, &token, TokenTransport::Http).await;
    }
    if let Ok(loaded) = read_credentials() {
        match loaded.credentials.auth {
            Auth::Bearer { token } => {
                return verify_against(ctx, token.as_str(), TokenTransport::Http).await;
            }
        }
    }
    check_aggregate(ctx).await
}

async fn verify_against(
    ctx: &CheckContext,
    token: &str,
    transport: TokenTransport,
) -> CheckOutcome {
    let mut conn = match ctx.pool.acquire().await {
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
    CheckOutcome::token_verification_failed(transport, err.into())
}

async fn check_aggregate(ctx: &CheckContext) -> CheckOutcome {
    let mut conn = match ctx.pool.acquire().await {
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

/// Variants `verify_token` cannot produce (`LocalPrincipalMissing`,
/// `InsufficientScope`) collapse into `DatabaseUnavailable` with their
/// rendered message — defensive arms, unreachable in practice.
impl From<AuthError> for TokenFailureReason {
    fn from(err: AuthError) -> Self {
        match err {
            AuthError::InvalidToken { .. } => Self::Invalid,
            AuthError::TokenRevoked { .. } => Self::Revoked,
            AuthError::TokenExpired { .. } => Self::Expired,
            AuthError::PrincipalNotFound { .. } => Self::PrincipalMissing,
            AuthError::DatabaseUnavailable { context, .. } => Self::DatabaseUnavailable { context },
            other => Self::DatabaseUnavailable {
                context: format!("unexpected AuthError variant: {other}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_skipped_stdio_is_skip() {
        assert!(matches!(
            &CheckOutcome::token_skipped_stdio(),
            CheckOutcome::Skip {
                name: CheckName::ValidTokenExists,
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
                    name: CheckName::ValidTokenExists,
                    detail: CheckDetail::TokenVerified { transport: t },
                } if *t == transport,
            ));
        }
    }

    #[test]
    fn test_token_verification_failed_is_fail_with_remediation() {
        let outcome = CheckOutcome::token_verification_failed(
            TokenTransport::Http,
            TokenFailureReason::Revoked,
        );
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                name: CheckName::ValidTokenExists,
                detail: CheckDetail::TokenVerificationFailed {
                    transport: TokenTransport::Http,
                    reason: TokenFailureReason::Revoked,
                },
                remediation: Some(CheckRemediation::RunTribalTokenCreate),
            },
        ));
    }

    #[test]
    fn test_token_aggregate_warn_is_warn() {
        assert!(matches!(
            &CheckOutcome::token_aggregate_warn(),
            CheckOutcome::Warn {
                name: CheckName::ValidTokenExists,
                detail: CheckDetail::TokenAggregateWarn,
                remediation: Some(CheckRemediation::RunTribalTokenCreate),
            },
        ));
    }

    #[test]
    fn test_no_active_tokens_is_fail() {
        assert!(matches!(
            &CheckOutcome::no_active_tokens(),
            CheckOutcome::Fail {
                name: CheckName::ValidTokenExists,
                detail: CheckDetail::NoActiveTokens,
                remediation: Some(CheckRemediation::RunTribalTokenCreate),
            },
        ));
    }

    #[test]
    fn test_token_aggregate_query_failed_is_fail_with_pg_isready() {
        let outcome = CheckOutcome::token_aggregate_query_failed("pool exhausted".into());
        assert!(matches!(
            &outcome,
            CheckOutcome::Fail {
                name: CheckName::ValidTokenExists,
                detail: CheckDetail::TokenAggregateQueryFailed { error },
                remediation: Some(CheckRemediation::CheckPgIsready),
            } if error == "pool exhausted",
        ));
    }

    #[test]
    fn test_auth_error_converts_to_token_failure_reason() {
        use tribal_domain::PrincipalId;

        let invalid: TokenFailureReason = AuthError::InvalidToken {
            token_hash: "h".into(),
        }
        .into();
        assert!(matches!(invalid, TokenFailureReason::Invalid));

        let revoked: TokenFailureReason = AuthError::TokenRevoked {
            token_hash: "h".into(),
        }
        .into();
        assert!(matches!(revoked, TokenFailureReason::Revoked));

        let expired: TokenFailureReason = AuthError::TokenExpired {
            token_hash: "h".into(),
        }
        .into();
        assert!(matches!(expired, TokenFailureReason::Expired));

        let pid: PrincipalId = "prin_550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid prin");
        let principal_missing: TokenFailureReason =
            AuthError::PrincipalNotFound { principal_id: pid }.into();
        assert!(matches!(
            principal_missing,
            TokenFailureReason::PrincipalMissing,
        ));

        let db: TokenFailureReason = AuthError::DatabaseUnavailable {
            context: "boom".into(),
            source: Box::new(std::io::Error::other("io")),
        }
        .into();
        assert!(matches!(
            db,
            TokenFailureReason::DatabaseUnavailable { context } if context == "boom",
        ));
    }
}
