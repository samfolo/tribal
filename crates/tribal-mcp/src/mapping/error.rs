//! `IntoMcpError` implementations for domain error types.
//!
//! Centralises the mapping from domain errors to MCP error codes.
//! Handlers never construct `McpToolError` directly for domain errors —
//! they call `.into_mcp_error()` instead.

use std::string::ToString;

use tribal_auth::AuthError;
use tribal_db::DbError;
use tribal_domain::{IdParseError, McpErrorCode};
use tribal_inference::InferenceError;

use crate::error::{IntoMcpError, McpToolError};

// ---------------------------------------------------------------------------
// DbError → McpToolError
// ---------------------------------------------------------------------------

impl IntoMcpError for DbError {
    fn into_mcp_error(self) -> McpToolError {
        let (code, message) = match &self {
            DbError::NotFound { entity, id } => {
                (McpErrorCode::NotFound, format!("{entity} not found: {id}"))
            }
            DbError::InvalidCursor { detail } => (
                McpErrorCode::InvalidArgument,
                format!("invalid cursor: {detail}"),
            ),
            DbError::PoolExhausted { pool_name } => (
                McpErrorCode::ResourceExhausted,
                format!("connection pool exhausted: {pool_name}"),
            ),
            DbError::UniqueViolation {
                table, constraint, ..
            } => (
                McpErrorCode::FailedPrecondition,
                format!("unique constraint violated on {table}.{constraint}"),
            ),
            DbError::QueryFailed { context, .. } => {
                (McpErrorCode::Internal, format!("query failed: {context}"))
            }
            DbError::SourceContextRejected { job_id, .. } => (
                McpErrorCode::Internal,
                format!("source context rejected an extraction commit for job {job_id}"),
            ),
            DbError::SourceContextUnreadable { job_id, .. } => (
                McpErrorCode::Internal,
                format!("source context unreadable for job {job_id}"),
            ),
            DbError::Migration { .. } => (McpErrorCode::Internal, "migration failed".to_owned()),
        };

        McpToolError {
            code,
            message,
            details: serde_json::json!({}),
        }
    }
}

// ---------------------------------------------------------------------------
// IdParseError → McpToolError
// ---------------------------------------------------------------------------

impl IntoMcpError for IdParseError {
    fn into_mcp_error(self) -> McpToolError {
        McpToolError {
            code: McpErrorCode::InvalidArgument,
            message: self.to_string(),
            details: serde_json::json!({}),
        }
    }
}

// ---------------------------------------------------------------------------
// InferenceError → McpToolError
// ---------------------------------------------------------------------------

impl IntoMcpError for InferenceError {
    fn into_mcp_error(self) -> McpToolError {
        McpToolError {
            code: McpErrorCode::Internal,
            message: self.to_string(),
            details: serde_json::json!({}),
        }
    }
}

// ---------------------------------------------------------------------------
// AuthError → McpToolError
// ---------------------------------------------------------------------------

impl IntoMcpError for AuthError {
    fn into_mcp_error(self) -> McpToolError {
        let (code, details) = match &self {
            AuthError::InvalidToken { .. }
            | AuthError::TokenRevoked { .. }
            | AuthError::TokenExpired { .. }
            | AuthError::AudienceMismatch { .. } => {
                (McpErrorCode::Unauthenticated, serde_json::json!({}))
            }
            AuthError::PrincipalNotFound { .. }
            | AuthError::LocalPrincipalMissing { .. }
            | AuthError::DatabaseUnavailable { .. } => {
                (McpErrorCode::Internal, serde_json::json!({}))
            }
            AuthError::InsufficientScope {
                required_scope,
                granted_scopes,
            } => (
                McpErrorCode::PermissionDenied,
                serde_json::json!({
                    "required_scope": required_scope.to_string(),
                    "granted_scopes": granted_scopes.iter().map(ToString::to_string).collect::<Vec<_>>(),
                }),
            ),
        };

        McpToolError {
            code,
            message: self.to_string(),
            details,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- DbError ----------------------------------------------------------

    #[test]
    fn test_db_not_found_maps_to_not_found() {
        let err = DbError::NotFound {
            entity: "job",
            id: "job_abc".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::NotFound);
        assert!(mcp.message.contains("job not found"));
    }

    #[test]
    fn test_db_invalid_cursor_maps_to_invalid_argument() {
        let err = DbError::InvalidCursor {
            detail: "bad cursor".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::InvalidArgument);
        assert!(mcp.message.contains("invalid cursor"));
    }

    #[test]
    fn test_db_pool_exhausted_maps_to_resource_exhausted() {
        let err = DbError::PoolExhausted { pool_name: "mcp" };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::ResourceExhausted);
        assert!(mcp.message.contains("pool exhausted"));
    }

    #[test]
    fn test_db_unique_violation_maps_to_failed_precondition() {
        let err = DbError::UniqueViolation {
            table: "knowledge_items".to_owned(),
            constraint: "knowledge_items_pkey".to_owned(),
            detail: "duplicate".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::FailedPrecondition);
        assert!(mcp.message.contains("unique constraint violated"));
    }

    #[test]
    fn test_db_query_failed_maps_to_internal() {
        let err = DbError::QueryFailed {
            context: "fetching job".to_owned(),
            source: sqlx::Error::RowNotFound,
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
        assert!(mcp.message.contains("query failed"));
    }

    #[test]
    fn test_db_migration_maps_to_internal() {
        let err = DbError::Migration {
            source: sqlx::migrate::MigrateError::VersionMissing(1),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
        assert!(mcp.message.contains("migration failed"));
    }

    // -- IdParseError -----------------------------------------------------

    #[test]
    fn test_id_missing_prefix_maps_to_invalid_argument() {
        let err = IdParseError::MissingPrefix {
            expected: "ki",
            input: "bad_id".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::InvalidArgument);
    }

    #[test]
    fn test_id_invalid_uuid_maps_to_invalid_argument() {
        let err = IdParseError::InvalidUuid {
            input: "ki_not-a-uuid".to_owned(),
            source: "not-a-uuid".parse::<uuid::Uuid>().unwrap_err(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::InvalidArgument);
    }

    // -- InferenceError ---------------------------------------------------

    #[test]
    fn test_provider_unavailable_maps_to_internal() {
        let err = InferenceError::provider_unavailable("ollama", "connection refused");
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
        assert!(mcp.message.contains("ollama"));
    }

    #[test]
    fn test_embedding_failed_maps_to_internal() {
        let err = InferenceError::EmbeddingFailed {
            model: "nomic-embed-text".to_owned(),
            context: "generating query embedding".to_owned(),
            source: None,
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
        assert!(mcp.message.contains("nomic-embed-text"));
    }

    #[test]
    fn test_llm_call_failed_maps_to_internal() {
        let err = InferenceError::LlmCallFailed {
            model: "claude-sonnet".to_owned(),
            context: "extraction".to_owned(),
            source: None,
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
        assert!(mcp.message.contains("claude-sonnet"));
    }

    #[test]
    fn test_response_parse_failed_maps_to_internal() {
        let err = InferenceError::ResponseParseFailed {
            expected_shape: "JSON object".to_owned(),
            actual: "plain text".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
        assert!(mcp.message.contains("JSON object"));
    }

    // -- AuthError --------------------------------------------------------

    #[test]
    fn test_auth_invalid_token_maps_to_unauthenticated() {
        let err = AuthError::InvalidToken {
            token_hash: "abc".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Unauthenticated);
    }

    #[test]
    fn test_auth_token_revoked_maps_to_unauthenticated() {
        let err = AuthError::TokenRevoked {
            token_hash: "abc".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Unauthenticated);
    }

    #[test]
    fn test_auth_token_expired_maps_to_unauthenticated() {
        let err = AuthError::TokenExpired {
            token_hash: "abc".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Unauthenticated);
    }

    #[test]
    fn test_auth_principal_not_found_maps_to_internal() {
        let err = AuthError::PrincipalNotFound {
            principal_id: tribal_domain::PrincipalId::new(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
    }

    #[test]
    fn test_auth_local_principal_missing_maps_to_internal() {
        let err = AuthError::LocalPrincipalMissing {
            principal_key: "principal:local".to_owned(),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
    }

    #[test]
    fn test_auth_database_unavailable_maps_to_internal() {
        let err = AuthError::DatabaseUnavailable {
            context: "test".to_owned(),
            source: Box::new(std::io::Error::other("boom")),
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::Internal);
    }

    #[test]
    fn test_auth_insufficient_scope_maps_to_permission_denied() {
        let err = AuthError::InsufficientScope {
            required_scope: tribal_domain::Scope::parse("tribal:write").unwrap(),
            granted_scopes: vec![tribal_domain::Scope::parse("tribal.knowledge:read").unwrap()],
        };
        let mcp = err.into_mcp_error();
        assert_eq!(mcp.code, McpErrorCode::PermissionDenied);
        assert_eq!(mcp.message, "insufficient scope: requires tribal:write");
        assert_eq!(mcp.details["required_scope"], "tribal:write");
        assert_eq!(mcp.details["granted_scopes"][0], "tribal.knowledge:read");
    }

    #[test]
    fn test_auth_insufficient_scope_full_chain_to_protocol_error() {
        use rmcp::model::ErrorCode;

        let err = AuthError::InsufficientScope {
            required_scope: tribal_domain::Scope::parse("tribal:write").unwrap(),
            granted_scopes: vec![tribal_domain::Scope::parse("tribal.knowledge:read").unwrap()],
        };
        let protocol = err.into_mcp_error().into_protocol_error();

        assert_eq!(protocol.code, ErrorCode::INVALID_REQUEST);
        assert_eq!(
            protocol.message,
            "insufficient scope: requires tribal:write"
        );

        let data = protocol.data.expect("data should carry structured error");
        assert_eq!(data["code"], "permission_denied");
    }
}
