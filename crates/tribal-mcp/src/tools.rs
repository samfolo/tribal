use std::sync::{Arc, LazyLock};

use rmcp::model::Tool;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INPUT_SCHEMA_PARSE_FAILED: &str = "invariant: embedded input schema must be valid JSON";
const OUTPUT_SCHEMA_PARSE_FAILED: &str = "invariant: embedded output schema must be valid JSON";

// ---------------------------------------------------------------------------
// ToolEntry (compile-time source)
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
// ParsedToolEntry (runtime registry)
// ---------------------------------------------------------------------------

pub(crate) struct ParsedToolEntry {
    pub(crate) name: &'static str,
    pub(crate) title: &'static str,
    pub(crate) description: &'static str,
    pub(crate) input_schema: serde_json::Value,
    pub(crate) output_schema: serde_json::Value,
    pub(crate) required_scope: &'static str,
}

// ---------------------------------------------------------------------------
// TOOLS (compile-time source)
// ---------------------------------------------------------------------------

pub(crate) static TOOLS: &[ToolEntry] = &[
    ToolEntry {
        name: "tribal_set_context",
        title: "Tribal: Set Session Context",
        description: "Set or override session-level context for Tribal. Use this at the start of a session to declare your model identity, or when switching to a different project.\n\nSession context is used as the default for all subsequent tool calls. For example, setting a project here means tribal_ingest and tribal_discover will use it automatically without needing project_id on every call.\n\nThe server resolves what it can at connection start (project from git remote, principal from auth). Use this tool to fill in what the server cannot infer (model name, provider) or to override what it resolved (e.g., switching projects).",
        input_schema: include_str!("schemas/set_context/input.json"),
        output_schema: include_str!("schemas/set_context/output.json"),
        required_scope: "tribal:write",
    },
    ToolEntry {
        name: "tribal_ingest",
        title: "Tribal: Ingest Knowledge",
        description: "Submit raw text for knowledge extraction into Tribal. The system extracts structured knowledge items (facts, heuristics, procedures, decision records), detects duplicates, identifies relationships with existing knowledge, and stores the results.\n\nThis is an asynchronous operation. Returns a job_id immediately. Use tribal_job_status to poll for completion.\n\nUse this tool when you've learned something worth preserving: a debugging insight, an architectural decision, a reusable pattern, a gotcha about a library, or any experience that would help you or another agent working on this codebase in the future.\n\nDo NOT use this for storing code snippets, file contents, or documentation. Tribal stores knowledge *about* work, not the artefacts themselves.\n\nProject, model, and principal are sourced from session context (see tribal_set_context). You only need to provide the content itself.",
        input_schema: include_str!("schemas/ingest/input.json"),
        output_schema: include_str!("schemas/ingest/output.json"),
        required_scope: "tribal.knowledge:write",
    },
    ToolEntry {
        name: "tribal_discover",
        title: "Tribal: Discover Knowledge",
        description: "Search Tribal's knowledge base using natural language. Returns knowledge items ranked by semantic similarity to your query, with optional structured filters to narrow results.\n\nUse this as your first step when you need context: before starting work on a feature, debugging an issue, or making a design decision. Ask questions the way you'd ask a colleague \u{2014} \"What do I know about connection pooling in this project?\" or \"Have I seen this async deadlock pattern before?\"\n\nSemantic search is the primary mechanism. Filters (project, kind, tags, time) narrow the candidate set but are not required. If you need to understand an item's evidence, contradictions, or derivation chain, follow up with tribal_explore using the item's ID.\n\nSuperseded items (replaced by newer understanding) are excluded by default. Set include_superseded to true for the historical picture.\n\nResults include standing (evidential profile) when requested, which summarises each item's support count, contradiction count, observation frequency, and diversity of supporting evidence.",
        input_schema: include_str!("schemas/discover/input.json"),
        output_schema: include_str!("schemas/discover/output.json"),
        required_scope: "tribal.knowledge:read",
    },
    ToolEntry {
        name: "tribal_explore",
        title: "Tribal: Explore Relationships",
        description: "Traverse the relationship graph from a specific knowledge item. Use this after tribal_discover to understand an item's context: what supports it, what contradicts it, what it was derived from, or what it supersedes.\n\nTypical workflow:\n1. tribal_discover finds relevant items\n2. Pick an item with interesting standing (high support, or contradictions)\n3. tribal_explore to see the evidence, contradictions, or derivation chain\n\nDirection controls traversal:\n- \"inbound\": What do others assert about this item? (supports, contradictions, what supersedes it)\n- \"outbound\": What does this item assert about others? (what it's derived from, what it supports)\n- \"both\": Full neighbourhood in all directions\n\nRelation types:\n- \"supports\": Evidence that reinforces the item\n- \"contradicts\": Evidence that challenges the item\n- \"supersedes\": A newer item that replaces this one\n- \"derived_from\": Provenance \u{2014} what input was used to produce this item\n\nDepth controls hops: depth 1 = direct relations, depth 2 = relations of relations. Higher depth gives more context but more results. Depth is capped at 3 to avoid mixing unrelated evidence across distant graph regions \u{2014} use multiple targeted calls for deeper investigation.",
        input_schema: include_str!("schemas/explore/input.json"),
        output_schema: include_str!("schemas/explore/output.json"),
        required_scope: "tribal.knowledge:read",
    },
    ToolEntry {
        name: "tribal_get_item",
        title: "Tribal: Get Knowledge Item by ID",
        description: "Retrieve one or more knowledge items by their IDs. Use this when you have a specific item ID \u{2014} from a standing field (newest_supporting_id, newest_contradicting_id, superseded_by), a previous session, or a cross-reference \u{2014} and need the full item.\n\nFor semantic search, use tribal_discover. For relationship traversal, use tribal_explore. This tool is for direct lookup when you already know what you want.\n\nThe response is keyed by item ID. Missing or unknown IDs map to null.",
        input_schema: include_str!("schemas/get_item/input.json"),
        output_schema: include_str!("schemas/get_item/output.json"),
        required_scope: "tribal.knowledge:read",
    },
    ToolEntry {
        name: "tribal_feedback",
        title: "Tribal: Rate Retrieval Quality",
        description: "Record a quality signal about a retrieval session. Use this when Tribal's knowledge meaningfully helped (or failed to help) your current task.\n\nThis is NOT about rating individual items \u{2014} item-level signals are captured through the Supports/Contradicts relationship system during ingest. This is about rating the *combination of items returned for a query, assembled in a particular way*.\n\nRate \"positive\" when: Tribal surfaced knowledge that directly informed your approach, saved you from a known pitfall, or provided context that improved your decision-making.\n\nRate \"negative\" when: The query should have found relevant knowledge but didn't, or the returned items were irrelevant or misleading for the task at hand.\n\nFeedback builds an organic eval dataset. Be selective \u{2014} only rate when the signal is clear. If no trace_id is available from the retrieval response, do not submit feedback rather than fabricating a trace_id \u{2014} incomplete feedback is noise.",
        input_schema: include_str!("schemas/feedback/input.json"),
        output_schema: include_str!("schemas/feedback/output.json"),
        required_scope: "tribal.knowledge:write",
    },
    ToolEntry {
        name: "tribal_job_status",
        title: "Tribal: Check Ingest Job Status",
        description: "Check the progress of an ingest job submitted via tribal_ingest.\n\nJob lifecycle: queued \u{2192} extracting \u{2192} triaging \u{2192} relating \u{2192} completed/failed\n\nTerminal states:\n- \"completed\": Pipeline ran to conclusion. Check outcome for details:\n  - \"success\": All candidates triaged successfully and relations committed.\n  - \"partial\": Some triage tasks dead-lettered; relation task ran on a subset.\n  - \"empty\": Relation task ran with zero items to relate (all duplicates or all triage failures). If tasks_failed > 0, the pipeline likely failed at triage \u{2014} treat as degraded rather than \"nothing new\".\n- \"failed\": Pipeline could not complete. outcome = \"failure\". Check error context.\n\nSet wait_seconds to block until the job completes or the timeout expires. This collapses ingest + poll into a single round-trip for fast operations. With wait_seconds=0 (default), returns immediately with current status.",
        input_schema: include_str!("schemas/job_status/input.json"),
        output_schema: include_str!("schemas/job_status/output.json"),
        required_scope: "tribal.jobs:read",
    },
];

// ---------------------------------------------------------------------------
// PARSED_TOOLS (runtime registry)
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

fn to_json_object(value: &serde_json::Value) -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(
        value
            .as_object()
            .expect("schema must be a JSON object")
            .clone(),
    )
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
            serde_json::from_str::<serde_json::Value>(entry.input_schema)
                .unwrap_or_else(|e| panic!("{}: input schema invalid: {e}", entry.name));
            serde_json::from_str::<serde_json::Value>(entry.output_schema)
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
        let tools: Vec<serde_json::Value> = PARSED_TOOLS
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

        if !snapshot_path.exists() {
            fs::write(&snapshot_path, &snapshot).expect("write initial golden snapshot");
            return;
        }

        let existing = fs::read_to_string(&snapshot_path).expect("read golden snapshot");
        assert_eq!(
            existing, snapshot,
            "Golden snapshot mismatch. Run with UPDATE_SNAPSHOTS=1 to update."
        );
    }
}
