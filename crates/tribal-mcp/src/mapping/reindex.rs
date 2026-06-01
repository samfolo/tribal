//! Mapping types for the reindex operator tools.

use rmcp::model::{CallToolResult, Content};
use serde::{Deserialize, Serialize};

use crate::error::IntoCallToolResult;

const SERIALISE_REINDEX_RESPONSE: &str = "invariant: reindex response serialises";
const SERIALISE_CANCEL_RESPONSE: &str = "invariant: reindex cancel response serialises";
const SERIALISE_PRUNE_RESPONSE: &str = "invariant: reindex prune response serialises";

/// The MCP request for `tribal_reindex`.
#[derive(Debug, Deserialize)]
pub(crate) struct McpReindexRequest {
    /// The target embedding provider (for example `"ollama"` or `"openai"`).
    pub(crate) provider: String,
    /// The target embedding model.
    pub(crate) model: String,
    /// The target vector dimension; resolved from the model when omitted.
    #[serde(default)]
    pub(crate) dimensions: Option<u32>,
    /// The target endpoint; the provider's default when omitted.
    #[serde(default)]
    pub(crate) base_url: Option<String>,
    /// When true, resolve and estimate the target without creating a run.
    #[serde(default)]
    pub(crate) dry_run: bool,
}

/// The MCP response for `tribal_reindex`.
#[derive(Debug, Serialize)]
pub(crate) struct McpReindexResponse {
    /// The outcome: `plan` for a dry run, otherwise `created`, `unchanged`,
    /// `already_live`, or `lock_contended`.
    pub(crate) outcome: String,
    /// The run id, present for `created` and `already_live`.
    pub(crate) run_id: Option<String>,
    /// The resolved target provider.
    pub(crate) provider: String,
    /// The resolved target model.
    pub(crate) model: String,
    /// The resolved target dimension.
    pub(crate) dimensions: u32,
    /// The resolved, normalised target endpoint.
    pub(crate) base_url: String,
    /// The number of items the new geometry must embed.
    pub(crate) estimated_items: u64,
    /// The number of tags the new geometry must embed.
    pub(crate) estimated_tags: u64,
}

impl IntoCallToolResult for McpReindexResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = match &self.run_id {
            Some(id) => format!(
                "Reindex {}: run {id} ({} {}, {} dims)",
                self.outcome, self.provider, self.model, self.dimensions,
            ),
            None => format!(
                "Reindex {}: {} {}, {} dims",
                self.outcome, self.provider, self.model, self.dimensions,
            ),
        };
        let structured = serde_json::to_value(&self).expect(SERIALISE_REINDEX_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

/// The MCP response for `tribal_reindex_cancel`.
#[derive(Debug, Serialize)]
pub(crate) struct McpReindexCancelResponse {
    /// Whether a live run was transitioned to aborted.
    pub(crate) cancelled: bool,
    /// The aborted run's id, present only when a run was cancelled.
    pub(crate) run_id: Option<String>,
}

impl IntoCallToolResult for McpReindexCancelResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = match &self.run_id {
            Some(id) => format!("Reindex run {id} cancelled"),
            None => "No live reindex run to cancel".to_owned(),
        };
        let structured = serde_json::to_value(&self).expect(SERIALISE_CANCEL_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

/// The MCP response for `tribal_reindex_prune`.
#[derive(Debug, Serialize)]
pub(crate) struct McpReindexPruneResponse {
    /// The number of profiles transitioned to superseded.
    pub(crate) profiles_superseded: u64,
    /// The number of item embeddings deleted.
    pub(crate) embeddings_deleted: u64,
    /// The number of tag embeddings deleted.
    pub(crate) tag_embeddings_deleted: u64,
}

impl IntoCallToolResult for McpReindexPruneResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = format!(
            "Pruned {} superseded profile(s): {} item and {} tag embeddings deleted",
            self.profiles_superseded, self.embeddings_deleted, self.tag_embeddings_deleted,
        );
        let structured = serde_json::to_value(&self).expect(SERIALISE_PRUNE_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}
