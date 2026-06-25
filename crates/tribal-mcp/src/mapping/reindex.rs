//! Mapping types for the reindex operator tools.

use rmcp::model::{CallToolResult, Content};
use tribal_wire::mcp::{McpReindexCancelResponse, McpReindexPruneResponse, McpReindexResponse};

use crate::error::IntoCallToolResult;

const SERIALISE_REINDEX_RESPONSE: &str = "invariant: reindex response serialises";
const SERIALISE_CANCEL_RESPONSE: &str = "invariant: reindex cancel response serialises";
const SERIALISE_PRUNE_RESPONSE: &str = "invariant: reindex prune response serialises";

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
