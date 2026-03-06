use std::sync::{Arc, LazyLock};

use rmcp::model::Tool;
use serde_json::{Map, Value};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INPUT_SCHEMA_PARSE_FAILED: &str = "invariant: embedded input schema must be valid JSON";
const OUTPUT_SCHEMA_PARSE_FAILED: &str = "invariant: embedded output schema must be valid JSON";
const SCHEMA_MUST_BE_OBJECT: &str = "invariant: schema must be a JSON object";

// ---------------------------------------------------------------------------
// ToolEntry
// ---------------------------------------------------------------------------

pub(crate) struct ToolEntry {
    pub(crate) name: &'static str,
    pub(crate) title: &'static str,
    pub(crate) description: &'static str,
    pub(crate) input_schema: &'static str,
    pub(crate) output_schema: &'static str,
    pub(crate) required_scope: &'static str,
}

// ---------------------------------------------------------------------------
// ParsedToolEntry
// ---------------------------------------------------------------------------

pub(crate) struct ParsedToolEntry {
    pub(crate) name: &'static str,
    pub(crate) title: &'static str,
    pub(crate) description: &'static str,
    pub(crate) input_schema: Value,
    pub(crate) output_schema: Value,
    pub(crate) required_scope: &'static str,
}

// ---------------------------------------------------------------------------
// TOOLS
// ---------------------------------------------------------------------------

pub(crate) static TOOLS: &[ToolEntry] = &[
    ToolEntry {
        name: "tribal_set_context",
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
        input_schema: include_str!("schemas/set_context/input.json"),
        output_schema: include_str!("schemas/set_context/output.json"),
        required_scope: "tribal:write",
    },
    ToolEntry {
        name: "tribal_ingest",
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
        input_schema: include_str!("schemas/ingest/input.json"),
        output_schema: include_str!("schemas/ingest/output.json"),
        required_scope: "tribal.knowledge:write",
    },
    ToolEntry {
        name: "tribal_discover",
        title: "Tribal: Discover Knowledge",
        description: "\
Search Tribal's knowledge base using natural language. Returns \
knowledge items ranked by semantic similarity to your query, with \
optional structured filters to narrow results.

Use this as your first step when you need context: before starting \
work on a feature, debugging an issue, or making a design decision. \
Ask questions the way you'd ask a colleague — \"What do I know about \
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
        input_schema: include_str!("schemas/discover/input.json"),
        output_schema: include_str!("schemas/discover/output.json"),
        required_scope: "tribal.knowledge:read",
    },
    ToolEntry {
        name: "tribal_explore",
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
- \"derived_from\": Provenance — what input was used to produce this item

Depth controls hops: depth 1 = direct relations, depth 2 = relations \
of relations. Higher depth gives more context but more results. Depth \
is capped at 3 to avoid mixing unrelated evidence across distant \
graph regions — use multiple targeted calls for deeper investigation.",
        input_schema: include_str!("schemas/explore/input.json"),
        output_schema: include_str!("schemas/explore/output.json"),
        required_scope: "tribal.knowledge:read",
    },
    ToolEntry {
        name: "tribal_get_item",
        title: "Tribal: Get Knowledge Item by ID",
        description: "\
Retrieve one or more knowledge items by their IDs. Use this when you \
have a specific item ID — from a standing field \
(newest_supporting_id, newest_contradicting_id, superseded_by), a \
previous session, or a cross-reference — and need the full item.

For semantic search, use tribal_discover. For relationship traversal, \
use tribal_explore. This tool is for direct lookup when you already \
know what you want.

The response is keyed by item ID. Missing or unknown IDs map to null.",
        input_schema: include_str!("schemas/get_item/input.json"),
        output_schema: include_str!("schemas/get_item/output.json"),
        required_scope: "tribal.knowledge:read",
    },
    ToolEntry {
        name: "tribal_feedback",
        title: "Tribal: Rate Retrieval Quality",
        description: "\
Record a quality signal about a retrieval session. Use this when \
Tribal's knowledge meaningfully helped (or failed to help) your \
current task.

This is NOT about rating individual items — item-level signals are \
captured through the Supports/Contradicts relationship system during \
ingest. This is about rating the *combination of items returned for a \
query, assembled in a particular way*.

Rate \"positive\" when: Tribal surfaced knowledge that directly \
informed your approach, saved you from a known pitfall, or provided \
context that improved your decision-making.

Rate \"negative\" when: The query should have found relevant knowledge \
but didn't, or the returned items were irrelevant or misleading for \
the task at hand.

Feedback builds an organic eval dataset. Be selective — only rate \
when the signal is clear. If no trace_id is available from the \
retrieval response, do not submit feedback rather than fabricating a \
trace_id — incomplete feedback is noise.",
        input_schema: include_str!("schemas/feedback/input.json"),
        output_schema: include_str!("schemas/feedback/output.json"),
        required_scope: "tribal.knowledge:write",
    },
    ToolEntry {
        name: "tribal_job_status",
        title: "Tribal: Check Ingest Job Status",
        description: "\
Check the progress of an ingest job submitted via tribal_ingest.

Job lifecycle: queued → extracting → triaging → relating → completed/failed

Terminal states:
- \"completed\": Pipeline ran to conclusion. Check outcome for details:
  - \"success\": All candidates triaged successfully and relations committed.
  - \"partial\": Some triage tasks dead-lettered; relation task ran on a subset.
  - \"empty\": Relation task ran with zero items to relate (all duplicates \
or all triage failures). If tasks_failed > 0, the pipeline likely failed \
at triage — treat as degraded rather than \"nothing new\".
- \"failed\": Pipeline could not complete. outcome = \"failure\". Check error context.

Set wait_seconds to block until the job completes or the timeout \
expires. This collapses ingest + poll into a single round-trip for \
fast operations. With wait_seconds=0 (default), returns immediately \
with current status.",
        input_schema: include_str!("schemas/job_status/input.json"),
        output_schema: include_str!("schemas/job_status/output.json"),
        required_scope: "tribal.jobs:read",
    },
];

// ---------------------------------------------------------------------------
// PARSED_TOOLS
// ---------------------------------------------------------------------------

/// Sole runtime registry. All handler code consults this, never `TOOLS`
/// directly.
///
/// # Panics
///
/// Panics on first access if any embedded schema is not valid JSON.
pub(crate) static PARSED_TOOLS: LazyLock<Vec<ParsedToolEntry>> = LazyLock::new(|| {
    TOOLS
        .iter()
        .map(|t| ParsedToolEntry {
            name: t.name,
            title: t.title,
            description: t.description,
            input_schema: serde_json::from_str(t.input_schema).expect(INPUT_SCHEMA_PARSE_FAILED),
            output_schema: serde_json::from_str(t.output_schema).expect(OUTPUT_SCHEMA_PARSE_FAILED),
            required_scope: t.required_scope,
        })
        .collect()
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn to_json_object(value: &Value) -> Arc<Map<String, Value>> {
    Arc::new(value.as_object().expect(SCHEMA_MUST_BE_OBJECT).clone())
}

pub(crate) fn to_tool(entry: &ParsedToolEntry) -> Tool {
    Tool::new(
        entry.name,
        entry.description,
        to_json_object(&entry.input_schema),
    )
    .with_title(entry.title)
    .with_raw_output_schema(to_json_object(&entry.output_schema))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, env, fs, path::Path};

    use super::*;

    #[test]
    fn test_schema_validity() {
        for entry in TOOLS {
            serde_json::from_str::<Value>(entry.input_schema)
                .unwrap_or_else(|e| panic!("{}: input schema invalid: {e}", entry.name));
            serde_json::from_str::<Value>(entry.output_schema)
                .unwrap_or_else(|e| panic!("{}: output schema invalid: {e}", entry.name));
        }
    }

    #[test]
    fn test_schema_coverage() {
        let schema_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("schemas");

        let dirs_on_disk: BTreeSet<String> = fs::read_dir(&schema_dir)
            .expect("schemas/ directory must exist")
            .filter_map(|e| {
                let entry = e.ok()?;
                if entry.file_type().ok()?.is_dir() {
                    Some(entry.file_name().to_string_lossy().into_owned())
                } else {
                    None
                }
            })
            .collect();

        let registry_dirs: BTreeSet<String> = TOOLS
            .iter()
            .map(|t| {
                t.name
                    .strip_prefix("tribal_")
                    .expect("tool name must start with tribal_")
                    .to_owned()
            })
            .collect();

        assert_eq!(
            dirs_on_disk, registry_dirs,
            "bijection between schema directories and registry entries failed"
        );
    }

    #[test]
    fn test_schema_naming_convention() {
        let schema_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("schemas");

        for entry in TOOLS {
            let dir_name = entry
                .name
                .strip_prefix("tribal_")
                .expect("tool name must start with tribal_");

            let tool_dir = schema_dir.join(dir_name);
            assert!(
                tool_dir.join("input.json").exists(),
                "{dir_name}/input.json missing"
            );
            assert!(
                tool_dir.join("output.json").exists(),
                "{dir_name}/output.json missing"
            );
        }
    }

    #[test]
    fn test_dispatch_covers_all_tools() {
        use crate::server_handler::DISPATCHED_TOOLS;

        let registry: BTreeSet<&str> = PARSED_TOOLS.iter().map(|t| t.name).collect();
        let dispatched: BTreeSet<&str> = DISPATCHED_TOOLS.iter().copied().collect();

        assert_eq!(
            registry, dispatched,
            "PARSED_TOOLS and DISPATCHED_TOOLS must match exactly"
        );
    }

    #[test]
    fn test_get_tool_found() {
        let tool = PARSED_TOOLS
            .iter()
            .find(|t| t.name == "tribal_discover")
            .map(to_tool);
        assert!(tool.is_some());
        let tool = tool.unwrap();
        assert_eq!(tool.name.as_ref(), "tribal_discover");
        assert_eq!(tool.title.as_deref(), Some("Tribal: Discover Knowledge"));
    }

    #[test]
    fn test_get_tool_not_found() {
        let tool = PARSED_TOOLS
            .iter()
            .find(|t| t.name == "nonexistent")
            .map(to_tool);
        assert!(tool.is_none());
    }

    #[test]
    fn test_schema_golden_snapshot() {
        let tools: Vec<Value> = PARSED_TOOLS
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t.name,
                    "title": t.title,
                    "description": t.description,
                    "input_schema": t.input_schema,
                    "output_schema": t.output_schema,
                    "required_scope": t.required_scope,
                })
            })
            .collect();

        let snapshot = serde_json::to_string_pretty(&tools).expect("snapshot serialisation");

        let snapshot_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("schemas")
            .join("golden_snapshot.json");

        if env::var("UPDATE_SNAPSHOTS").is_ok() {
            fs::write(&snapshot_path, &snapshot).expect("write golden snapshot");
            return;
        }

        assert!(
            snapshot_path.exists(),
            "Golden snapshot missing. Run with UPDATE_SNAPSHOTS=1 to create it."
        );

        let existing = fs::read_to_string(&snapshot_path).expect("read golden snapshot");
        assert_eq!(
            existing, snapshot,
            "Golden snapshot mismatch. Run with UPDATE_SNAPSHOTS=1 to update."
        );
    }
}
