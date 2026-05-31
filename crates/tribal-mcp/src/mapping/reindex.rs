//! Mapping types for the reindex operator tools.

use rmcp::model::{CallToolResult, Content};
use serde::Serialize;

use crate::error::IntoCallToolResult;

const SERIALISE_CANCEL_RESPONSE: &str = "invariant: reindex cancel response serialises";
const SERIALISE_PRUNE_RESPONSE: &str = "invariant: reindex prune response serialises";

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
