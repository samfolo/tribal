//! One declaration per registered MCP tool and resource: the wire name, the
//! scope its dispatch requires, its presentation copy, and the request or
//! response types its schema derives from. [`crate::tools`] reads the tool
//! declarations to build its advertised registry; [`super`]'s crate-level
//! [`super::declare_first_party_mcp_contract!`] invocation selects the
//! subset generated for native clients.

use tribal_wire::mcp::{
    McpDiscoverRequest, McpDiscoverResponse, McpEmptyRequest, McpExploreRequest,
    McpExploreResponse, McpFeedbackRequest, McpFeedbackResponse, McpGetItemRequest,
    McpGetItemResponse, McpIngestRequest, McpIngestResponse, McpIngestionInputResponse,
    McpJobStatusRequest, McpJobStatusResponse, McpRecentIngestionsResponse,
    McpReindexCancelResponse, McpReindexPruneResponse, McpReindexRequest, McpReindexResponse,
    McpSetContextRequest, McpSetContextResponse,
};

use super::{McpResourceRead, McpToolCall, ResourcePresentation, ToolPresentation};
use crate::handlers::{
    INGESTION_INPUT_URI_TEMPLATE, INGESTIONS_REQUIRED_SCOPE, RECENT_INGESTIONS_URI_TEMPLATE,
};

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

/// The `tribal_set_context` tool declaration.
pub struct McpSetContextCall;

impl McpToolCall for McpSetContextCall {
    const NAME: &'static str = "tribal_set_context";
    const REQUIRED_SCOPE: &'static str = "tribal:write";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Set Session Context",
        description: "\
Set or override session-level context for Tribal. Use this at the \
start of a session to declare your model identity, or when switching \
to a different project.

Session context is used as the default for all subsequent tool calls. \
For example, setting a project here means tribal_ingest and \
tribal_discover will use it automatically without needing project_id \
on every call.

The server resolves what it can at connection start (project from git \
remote, principal from auth). Use this tool to fill in what the server \
cannot infer (model name, provider) or to override what it resolved \
(e.g., switching projects).",
    };
    type Request = McpSetContextRequest;
    type Response = McpSetContextResponse;
}

/// The `tribal_ingest` tool declaration.
pub struct McpIngestCall;

impl McpToolCall for McpIngestCall {
    const NAME: &'static str = "tribal_ingest";
    const REQUIRED_SCOPE: &'static str = "tribal.knowledge:write";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Ingest Knowledge",
        description: "\
Submit raw text for knowledge extraction into Tribal. The system \
extracts structured knowledge items (facts, heuristics, procedures, \
decision records), detects duplicates, identifies relationships with \
existing knowledge, and stores the results.

This is an asynchronous operation. Returns a job_id immediately. Use \
tribal_job_status to poll for completion.

Use this tool when you've learned something worth preserving: a \
debugging insight, an architectural decision, a reusable pattern, a \
gotcha about a library, or any experience that would help you or \
another agent working on this codebase in the future.

Do NOT use this for storing code snippets, file contents, or \
documentation. Tribal stores knowledge *about* work, not the \
artefacts themselves.

Project, model, and principal are sourced from session context (see \
tribal_set_context). You only need to provide the content itself.",
    };
    type Request = McpIngestRequest;
    type Response = McpIngestResponse;
}

/// The `tribal_discover` tool declaration.
pub struct McpDiscoverCall;

impl McpToolCall for McpDiscoverCall {
    const NAME: &'static str = "tribal_discover";
    const REQUIRED_SCOPE: &'static str = "tribal.knowledge:read";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Discover Knowledge",
        description: "\
Search Tribal's knowledge base using natural language. Returns \
knowledge items ranked by semantic similarity to your query, with \
optional structured filters to narrow results.

Use this as your first step when you need context: before starting \
work on a feature, debugging an issue, or making a design decision. \
Ask questions the way you'd ask a colleague: \"What do I know about \
connection pooling in this project?\" or \"Have I seen this async \
deadlock pattern before?\"

Semantic search is the primary mechanism. Filters (project, kind, \
tags, time) narrow the candidate set but are not required. If you \
need to understand an item's evidence, contradictions, or derivation \
chain, follow up with tribal_explore using the item's ID.

Superseded items (replaced by newer understanding) are excluded by \
default. Set include_superseded to true for the historical picture.

Results include standing (evidential profile) when requested, which \
summarises each item's support count, contradiction count, observation \
frequency, and diversity of supporting evidence.",
    };
    type Request = McpDiscoverRequest;
    type Response = McpDiscoverResponse;
}

/// The `tribal_explore` tool declaration.
pub struct McpExploreCall;

impl McpToolCall for McpExploreCall {
    const NAME: &'static str = "tribal_explore";
    const REQUIRED_SCOPE: &'static str = "tribal.knowledge:read";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Explore Relationships",
        description: "\
Traverse the relationship graph from a specific knowledge item. Use \
this after tribal_discover to understand an item's context: what \
supports it, what contradicts it, what it was derived from, or what \
it supersedes.

Typical workflow:
1. tribal_discover finds relevant items
2. Pick an item with interesting standing (high support, or contradictions)
3. tribal_explore to see the evidence, contradictions, or derivation chain

Direction controls traversal:
- \"inbound\": What do others assert about this item? (supports, contradictions, what supersedes it)
- \"outbound\": What does this item assert about others? (what it's derived from, what it supports)
- \"both\": Full neighbourhood in all directions

Relation types:
- \"supports\": Evidence that reinforces the item
- \"contradicts\": Evidence that challenges the item
- \"supersedes\": A newer item that replaces this one
- \"derived_from\": Provenance. The input used to produce this item

Depth controls hops: depth 1 = direct relations, depth 2 = relations \
of relations. Higher depth gives more context but more results. Depth \
is capped at 3 to avoid mixing unrelated evidence across distant \
graph regions; use multiple targeted calls for deeper investigation.",
    };
    type Request = McpExploreRequest;
    type Response = McpExploreResponse;
}

/// The `tribal_get_item` tool declaration.
pub struct McpGetItemCall;

impl McpToolCall for McpGetItemCall {
    const NAME: &'static str = "tribal_get_item";
    const REQUIRED_SCOPE: &'static str = "tribal.knowledge:read";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Get Knowledge Item by ID",
        description: "\
Retrieve one or more knowledge items by their IDs. Use this when you \
have a specific item ID (from a standing field, a previous session, \
or a cross-reference) and need the full item.

For semantic search, use tribal_discover. For relationship traversal, \
use tribal_explore. This tool is for direct lookup when you already \
know what you want.

The response is keyed by item ID. Missing or unknown IDs map to null.",
    };
    type Request = McpGetItemRequest;
    type Response = McpGetItemResponse;
}

/// The `tribal_feedback` tool declaration.
pub struct McpFeedbackCall;

impl McpToolCall for McpFeedbackCall {
    const NAME: &'static str = "tribal_feedback";
    const REQUIRED_SCOPE: &'static str = "tribal.knowledge:write";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Rate Retrieval Quality",
        description: "\
Record a quality signal about a retrieval session. Use this when \
Tribal's knowledge meaningfully helped (or failed to help) your \
current task.

This is NOT about rating individual items. Item-level signals are \
captured through the Supports/Contradicts relationship system during \
ingest. This is about rating the *combination of items returned for a \
query, assembled in a particular way*.

Rate \"positive\" when: Tribal surfaced knowledge that directly \
informed your approach, saved you from a known pitfall, or provided \
context that improved your decision-making.

Rate \"negative\" when: The query should have found relevant knowledge \
but didn't, or the returned items were irrelevant or misleading for \
the task at hand.

Feedback builds an organic eval dataset. Be selective: only rate \
when the signal is clear. If no trace_id is available from the \
retrieval response, do not submit feedback rather than fabricating a \
trace_id. Incomplete feedback is noise.",
    };
    type Request = McpFeedbackRequest;
    type Response = McpFeedbackResponse;
}

/// The `tribal_job_status` tool declaration.
pub struct McpJobStatusCall;

impl McpToolCall for McpJobStatusCall {
    const NAME: &'static str = "tribal_job_status";
    const REQUIRED_SCOPE: &'static str = "tribal.jobs:read";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Check Ingest Job Status",
        description: "\
Check the progress of an ingest job submitted via tribal_ingest.

Job lifecycle: queued → extracting → triaging → relating → completed/failed

Terminal states:
- \"completed\": Pipeline ran to conclusion. Check outcome for details:
  - \"success\": All candidates triaged successfully and relations committed.
  - \"partial\": Some triage tasks failed permanently; the relation task ran on a subset.
  - \"empty\": Relation task ran with zero items to relate (all duplicates \
or all triage failures). If tasks_failed > 0, the pipeline likely failed \
at triage; treat as degraded rather than \"nothing new\".
- \"failed\": Pipeline could not complete. outcome = \"failure\". Check error context.

Set wait_seconds to block until the job completes or the timeout \
expires. This collapses ingest + poll into a single round-trip for \
fast operations. With wait_seconds=0 (default), returns immediately \
with current status.",
    };
    type Request = McpJobStatusRequest;
    type Response = McpJobStatusResponse;
}

/// The `tribal_reindex` tool declaration.
pub struct McpReindexCall;

impl McpToolCall for McpReindexCall {
    const NAME: &'static str = "tribal_reindex";
    const REQUIRED_SCOPE: &'static str = "tribal.embedding:execute";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Reindex Embeddings",
        description: "\
Start a reindex to a new embedding geometry, naming the target provider, \
model, and dimension on the command. Reads and writes continue against the \
active profile while the new space fills; the swap is atomic. An unchanged \
target is a no-op. Operator-only; the worker drives the run to completion.",
    };
    type Request = McpReindexRequest;
    type Response = McpReindexResponse;
}

/// The `tribal_reindex_cancel` tool declaration.
pub struct McpReindexCancelCall;

impl McpToolCall for McpReindexCancelCall {
    const NAME: &'static str = "tribal_reindex_cancel";
    const REQUIRED_SCOPE: &'static str = "tribal.embedding:execute";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Cancel Reindex",
        description: "\
Cancel the live reindex run, if any. The run is aborted and its building \
profile is failed at the next task boundary; the active profile, and every \
read and write against it, is untouched. Reindex is single-flight, so there \
is at most one live run. Operator-only.",
    };
    type Request = McpEmptyRequest;
    type Response = McpReindexCancelResponse;
}

/// The `tribal_reindex_prune` tool declaration.
pub struct McpReindexPruneCall;

impl McpToolCall for McpReindexPruneCall {
    const NAME: &'static str = "tribal_reindex_prune";
    const REQUIRED_SCOPE: &'static str = "tribal.embedding:execute";
    const PRESENTATION: ToolPresentation = ToolPresentation {
        title: "Tribal: Prune Reindexes",
        description: "\
Reclaim storage from past reindexes. Every non-active complete profile and \
every failed profile is superseded, and their embeddings are deleted; the \
active profile and run history are untouched. Operator-only.",
    };
    type Request = McpEmptyRequest;
    type Response = McpReindexPruneResponse;
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// The `tribal://ingestions/recent` resource declaration.
pub struct McpRecentIngestionsResource;

impl McpResourceRead for McpRecentIngestionsResource {
    const URI_TEMPLATE: &'static str = RECENT_INGESTIONS_URI_TEMPLATE;
    const REQUIRED_SCOPE: &'static str = INGESTIONS_REQUIRED_SCOPE;
    const PRESENTATION: ResourcePresentation = ResourcePresentation {
        name: "recent_ingestions",
        title: "Recent ingestions",
        description: "The caller's recent ingestion jobs, newest first, with bounded previews",
        mime_type: "application/json",
    };
    type Response = McpRecentIngestionsResponse;
}

/// The `tribal://ingestions/{job_id}/input` resource declaration.
pub struct McpIngestionInputResource;

impl McpResourceRead for McpIngestionInputResource {
    const URI_TEMPLATE: &'static str = INGESTION_INPUT_URI_TEMPLATE;
    const REQUIRED_SCOPE: &'static str = INGESTIONS_REQUIRED_SCOPE;
    const PRESENTATION: ResourcePresentation = ResourcePresentation {
        name: "ingestion_input",
        title: "Ingestion input",
        description: "The verbatim content of one of the caller's ingestions",
        mime_type: "application/json",
    };
    type Response = McpIngestionInputResponse;
}
