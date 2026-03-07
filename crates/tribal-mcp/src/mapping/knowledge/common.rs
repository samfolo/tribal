//! Shared MCP output types for knowledge-related tools.
//!
//! These types appear in the responses of `tribal_discover`, `tribal_explore`,
//! and `tribal_get_item`. ID fields carry prefixes applied via `Display` on
//! the domain ID types.

use chrono::{DateTime, Utc};
use serde::Serialize;
use tribal_domain::{Confidence, KnowledgeItem, KnowledgeKind, Reference, ReferenceKind, Standing};

// ---------------------------------------------------------------------------
// McpSourceType
// ---------------------------------------------------------------------------

/// Source type on the MCP surface.
///
/// Maps from the internal `source_context` JSONB discriminator to the
/// three values exposed to clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum McpSourceType {
    /// Captured via a model-mediated pipeline (e.g. agent ingesting via MCP).
    AgentMediated,
    /// Human-initiated capture without model mediation.
    Manual,
    /// Produced by synthesis or compaction from existing items.
    Derived,
}

// ---------------------------------------------------------------------------
// McpSourceContext
// ---------------------------------------------------------------------------

/// Flattened provenance summary extracted from the opaque JSONB
/// `source_context` column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct McpSourceContext {
    /// Discriminator — always present.
    #[serde(rename = "type")]
    pub(crate) source_type: McpSourceType,

    /// Inference provider name (e.g. `"anthropic"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provider: Option<String>,

    /// Model identifier (e.g. `"claude-opus-4-6"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<String>,
}

impl From<&serde_json::Value> for McpSourceContext {
    fn from(value: &serde_json::Value) -> Self {
        let source_type = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .map_or(McpSourceType::Manual, |t| match t {
                "agent_mediated" => McpSourceType::AgentMediated,
                "derived" => McpSourceType::Derived,
                // "manual_capture", "file_watch", and unknown values map to Manual.
                _ => McpSourceType::Manual,
            });

        let provider = value
            .get("provider")
            .and_then(serde_json::Value::as_str)
            .map(String::from);

        let model = value
            .get("model")
            .and_then(serde_json::Value::as_str)
            .map(String::from);

        Self {
            source_type,
            provider,
            model,
        }
    }
}

// ---------------------------------------------------------------------------
// McpKnowledgeItem
// ---------------------------------------------------------------------------

/// MCP-surface representation of a knowledge item.
///
/// All ID fields carry their type prefix (e.g. `ki_`, `proj_`, `ep_`).
/// The `principal_key` field is the human-readable string, not the UUID.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct McpKnowledgeItem {
    pub(crate) id: String,
    pub(crate) project_id: String,
    pub(crate) principal_key: String,
    pub(crate) kind: KnowledgeKind,
    pub(crate) content: String,
    pub(crate) tags: Vec<String>,
    pub(crate) confidence: Confidence,
    pub(crate) source_context: McpSourceContext,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) episode_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) capture_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) capture_branch: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
}

impl McpKnowledgeItem {
    /// Builds an `McpKnowledgeItem` from a domain `KnowledgeItem` and the
    /// resolved `principal_key` string.
    ///
    /// A named constructor is needed because the domain type stores
    /// `principal_id` (a UUID) while the MCP surface requires the
    /// human-readable `principal_key`.
    #[must_use]
    pub(crate) fn from_item_with_principal_key(item: &KnowledgeItem, principal_key: &str) -> Self {
        Self {
            id: item.id().to_string(),
            project_id: item.project_id().to_string(),
            principal_key: principal_key.to_owned(),
            kind: item.kind(),
            content: item.content().to_owned(),
            tags: item.tags().to_vec(),
            confidence: item.confidence(),
            source_context: McpSourceContext::from(item.source_context()),
            episode_id: item.episode_id().map(Into::into),
            capture_commit: item.capture_commit().map(String::from),
            capture_branch: item.capture_branch().map(String::from),
            created_at: item.created_at(),
        }
    }
}

// ---------------------------------------------------------------------------
// McpStanding
// ---------------------------------------------------------------------------

/// Computed evidential profile on the MCP surface.
///
/// ID fields are rendered as prefixed strings. Optional IDs are omitted
/// from serialised JSON when absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct McpStanding {
    pub(crate) supporting_count: u32,
    pub(crate) contradicting_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) superseded_by: Option<String>,
    pub(crate) observation_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) newest_supporting_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) newest_contradicting_id: Option<String>,
    pub(crate) supporting_episode_count: u32,
    pub(crate) supporting_project_count: u32,
}

impl From<&Standing> for McpStanding {
    fn from(s: &Standing) -> Self {
        Self {
            supporting_count: s.supporting_count(),
            contradicting_count: s.contradicting_count(),
            superseded_by: s.superseded_by().map(Into::into),
            observation_count: s.observation_count(),
            newest_supporting_id: s.newest_supporting_id().map(Into::into),
            newest_contradicting_id: s.newest_contradicting_id().map(Into::into),
            supporting_episode_count: s.supporting_episode_count(),
            supporting_project_count: s.supporting_project_count(),
        }
    }
}

// ---------------------------------------------------------------------------
// McpReference
// ---------------------------------------------------------------------------

/// MCP-surface representation of a reference.
///
/// Projects a subset of the domain `Reference` fields — excludes
/// `knowledge_item_id`, `project_id`, and `branch`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct McpReference {
    pub(crate) id: String,
    pub(crate) kind: ReferenceKind,
    pub(crate) value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) commit: Option<String>,
}

impl From<&Reference> for McpReference {
    fn from(r: &Reference) -> Self {
        Self {
            id: r.id().to_string(),
            kind: r.kind(),
            value: r.value().to_owned(),
            description: r.description().map(String::from),
            commit: r.commit().map(String::from),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{
        EpisodeId, KnowledgeItemId, PrincipalId, ProjectId, ReferenceId, Standing,
    };

    use super::*;

    // -- McpKnowledgeItem -------------------------------------------------

    #[test]
    fn test_knowledge_item_from_domain_with_all_fields() {
        let ki_id = KnowledgeItemId::new();
        let proj_id = ProjectId::new();
        let ep_id = EpisodeId::new();

        let item = KnowledgeItem::builder()
            .id(ki_id)
            .project_id(proj_id)
            .principal_id(PrincipalId::new())
            .kind(KnowledgeKind::Fact)
            .content("auth uses JWT".to_owned())
            .tags(vec!["auth".to_owned()])
            .confidence(Confidence::Verified)
            .source_context(serde_json::json!({"type": "agent_mediated", "provider": "anthropic"}))
            .episode_id(Some(ep_id))
            .capture_commit(Some("abc123".to_owned()))
            .capture_branch(Some("main".to_owned()))
            .created_at(chrono::Utc::now())
            .build();

        let mcp = McpKnowledgeItem::from_item_with_principal_key(&item, "user:sam");

        // ID prefixes
        assert!(mcp.id.starts_with("ki_"));
        assert_eq!(mcp.id, ki_id.to_string());
        assert!(mcp.project_id.starts_with("proj_"));
        assert_eq!(mcp.project_id, proj_id.to_string());
        assert!(mcp.episode_id.as_ref().unwrap().starts_with("ep_"));
        assert_eq!(mcp.episode_id.as_deref(), Some(ep_id.to_string().as_str()));

        // principal_key is passed through, not the UUID
        assert_eq!(mcp.principal_key, "user:sam");

        // Domain fields
        assert_eq!(mcp.kind, KnowledgeKind::Fact);
        assert_eq!(mcp.content, "auth uses JWT");
        assert_eq!(mcp.tags, vec!["auth"]);
        assert_eq!(mcp.confidence, Confidence::Verified);

        // Source context conversion is delegated
        assert_eq!(mcp.source_context.source_type, McpSourceType::AgentMediated);
        assert_eq!(mcp.source_context.provider.as_deref(), Some("anthropic"));

        // Optional fields present
        assert_eq!(mcp.capture_commit.as_deref(), Some("abc123"));
        assert_eq!(mcp.capture_branch.as_deref(), Some("main"));
    }

    #[test]
    fn test_knowledge_item_from_domain_optional_fields_absent() {
        let item = KnowledgeItem::builder()
            .id(KnowledgeItemId::new())
            .project_id(ProjectId::new())
            .principal_id(PrincipalId::new())
            .kind(KnowledgeKind::Heuristic)
            .content("test".to_owned())
            .confidence(Confidence::Inferred)
            .source_context(serde_json::json!({}))
            .created_at(chrono::Utc::now())
            .build();

        let mcp = McpKnowledgeItem::from_item_with_principal_key(&item, "user:test");

        assert!(mcp.episode_id.is_none());
        assert!(mcp.capture_commit.is_none());
        assert!(mcp.capture_branch.is_none());
    }

    // -- McpSourceContext --------------------------------------------------

    #[test]
    fn test_source_context_agent_mediated() {
        let json = serde_json::json!({
            "type": "agent_mediated",
            "provider": "anthropic",
            "model": "claude-opus-4-6",
        });
        let ctx = McpSourceContext::from(&json);
        assert_eq!(ctx.source_type, McpSourceType::AgentMediated);
        assert_eq!(ctx.provider.as_deref(), Some("anthropic"));
        assert_eq!(ctx.model.as_deref(), Some("claude-opus-4-6"));
    }

    #[test]
    fn test_source_context_manual_capture_maps_to_manual() {
        let json = serde_json::json!({"type": "manual_capture"});
        let ctx = McpSourceContext::from(&json);
        assert_eq!(ctx.source_type, McpSourceType::Manual);
        assert!(ctx.provider.is_none());
        assert!(ctx.model.is_none());
    }

    #[test]
    fn test_source_context_derived() {
        let json = serde_json::json!({"type": "derived"});
        let ctx = McpSourceContext::from(&json);
        assert_eq!(ctx.source_type, McpSourceType::Derived);
    }

    #[test]
    fn test_source_context_file_watch_maps_to_manual() {
        let json = serde_json::json!({"type": "file_watch"});
        let ctx = McpSourceContext::from(&json);
        assert_eq!(ctx.source_type, McpSourceType::Manual);
    }

    #[test]
    fn test_source_context_missing_type_maps_to_manual() {
        let json = serde_json::json!({});
        let ctx = McpSourceContext::from(&json);
        assert_eq!(ctx.source_type, McpSourceType::Manual);

        let null = serde_json::Value::Null;
        let ctx_null = McpSourceContext::from(&null);
        assert_eq!(ctx_null.source_type, McpSourceType::Manual);
    }

    // -- McpStanding ------------------------------------------------------

    #[test]
    fn test_standing_from_domain() {
        let sup_id = KnowledgeItemId::new();
        let con_id = KnowledgeItemId::new();
        let standing = Standing::builder()
            .supporting_count(3)
            .contradicting_count(1)
            .superseded_by(Some(KnowledgeItemId::new()))
            .observation_count(5)
            .newest_supporting_id(Some(sup_id))
            .newest_contradicting_id(Some(con_id))
            .supporting_episode_count(2)
            .supporting_project_count(1)
            .build();
        let mcp = McpStanding::from(&standing);
        assert_eq!(mcp.supporting_count, 3);
        assert_eq!(mcp.contradicting_count, 1);
        assert!(mcp.superseded_by.is_some());
        assert_eq!(
            mcp.newest_supporting_id.as_deref(),
            Some(sup_id.to_string().as_str())
        );
        assert!(
            mcp.newest_contradicting_id
                .as_ref()
                .unwrap()
                .starts_with("ki_")
        );
        assert_eq!(
            mcp.newest_contradicting_id.as_deref(),
            Some(con_id.to_string().as_str())
        );
    }

    #[test]
    fn test_standing_optional_ids_omitted_when_absent() {
        let standing = Standing::builder()
            .supporting_count(0)
            .contradicting_count(0)
            .observation_count(0)
            .supporting_episode_count(0)
            .supporting_project_count(0)
            .build();
        let mcp = McpStanding::from(&standing);
        let json = serde_json::to_value(&mcp).expect("serialises");
        assert!(!json.as_object().unwrap().contains_key("superseded_by"));
        assert!(
            !json
                .as_object()
                .unwrap()
                .contains_key("newest_supporting_id")
        );
        assert!(
            !json
                .as_object()
                .unwrap()
                .contains_key("newest_contradicting_id")
        );
    }

    // -- McpReference -----------------------------------------------------

    #[test]
    fn test_reference_from_domain() {
        let reference = Reference::builder()
            .id(ReferenceId::new())
            .knowledge_item_id(KnowledgeItemId::new())
            .kind(ReferenceKind::FilePath)
            .value("//src/auth.rs".to_owned())
            .description(Some("Auth middleware".to_owned()))
            .project_id(ProjectId::new())
            .commit(Some("abc123".to_owned()))
            .build();

        let mcp = McpReference::from(&reference);
        assert!(mcp.id.starts_with("ref_"));
        assert_eq!(mcp.kind, ReferenceKind::FilePath);
        assert_eq!(mcp.value, "//src/auth.rs");
        assert_eq!(mcp.description.as_deref(), Some("Auth middleware"));
        assert_eq!(mcp.commit.as_deref(), Some("abc123"));
    }
}
