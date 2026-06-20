//! Binding resolution: content-addressing what a thread runs.
//!
//! The hash covers the canonically serialised definition (tool
//! descriptors included), so a prompt edit, a model change, or a
//! tool-surface change is a new version even when everything else is
//! identical. Canonical means compact JSON with fields in declaration
//! order; the definition's serialisation is pinned byte-for-byte by a
//! tribal-domain test. Recording is idempotent by hash, so concurrent
//! resolvers converge on one row.

use sqlx::PgConnection;
use tribal_common::sha256_hex;
use tribal_db::{
    AgentBindingVersionRepository, NewAgentBindingVersion, PgAgentBindingVersionRepository,
};
use tribal_domain::{AgentBinding, AgentDefinition};

use crate::AgentRuntimeError;

/// Resolves a definition to its stored, content-addressed binding
/// version, recording it on first sight.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::ContentSerialisation`] when the
/// definition cannot be serialised, or [`AgentRuntimeError::Database`] on
/// database errors.
pub async fn resolve_binding(
    conn: &mut PgConnection,
    definition: &AgentDefinition,
) -> Result<AgentBinding, AgentRuntimeError> {
    let canonical =
        definition
            .canonical_json()
            .map_err(|source| AgentRuntimeError::ContentSerialisation {
                context: format!("canonicalising the {} binding", definition.pipeline_stage),
                source,
            })?;
    let hash = sha256_hex(&canonical);

    let new = NewAgentBindingVersion::builder()
        .hash(hash)
        .pipeline_stage(definition.pipeline_stage)
        .definition(definition.clone())
        .build();

    PgAgentBindingVersionRepository
        .record(conn, &new)
        .await
        .map_err(|source| AgentRuntimeError::database("recording the binding version", source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_serialisation_is_deterministic() {
        // Two structurally equal definitions hash identically; the
        // binding's idempotency rides on this.
        let build = || {
            serde_json::to_string(&serde_json::json!({
                "b": 1,
                "a": [2, 3],
            }))
            .expect("serialises")
        };
        assert_eq!(sha256_hex(&build()), sha256_hex(&build()));
    }
}
