mod error;
mod feedback;
mod job;
mod knowledge;
mod reindex;
mod session;

pub(crate) use job::recent_ingestion_to_wire;
pub(crate) use knowledge::relation_direction_from_db;
pub(crate) use session::{session_to_json, set_context_response};
// Wire DTOs live in `tribal-wire` (one source of truth, shared with clients);
// re-exported here so handlers keep referring to them as `crate::mapping::Mcp*`.
pub(crate) use tribal_wire::mcp::{
    McpDiscoverRequest, McpDiscoverResponse, McpDiscoveryResult, McpExplorationResult,
    McpExploreRequest, McpExploreResponse, McpFeedbackRequest, McpFeedbackResponse,
    McpGetItemEntry, McpGetItemRequest, McpGetItemResponse, McpIngestRequest, McpIngestResponse,
    McpIngestionInputResponse, McpJobStatusRequest, McpJobStatusResponse, McpKnowledgeItem,
    McpRecentIngestionsResponse, McpReference, McpReindexCancelResponse, McpReindexPruneResponse,
    McpReindexRequest, McpReindexResponse, McpSetContextRequest, McpStanding, RequestedProject,
};
