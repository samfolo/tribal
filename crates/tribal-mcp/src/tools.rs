use std::sync::{Arc, LazyLock};

use rmcp::model::Tool;
use serde_json::{Map, Value};
use tribal_domain::Scope;

use crate::contract::{
    McpToolCall,
    declarations::{
        McpDiscoverCall, McpExploreCall, McpFeedbackCall, McpGetItemCall, McpIngestCall,
        McpJobStatusCall, McpReindexCall, McpReindexCancelCall, McpReindexPruneCall,
        McpSetContextCall,
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INPUT_SCHEMA_PARSE_FAILED: &str = "invariant: embedded input schema must be valid JSON";
const OUTPUT_SCHEMA_PARSE_FAILED: &str = "invariant: embedded output schema must be valid JSON";
const SCHEMA_MUST_BE_OBJECT: &str = "invariant: schema must be a JSON object";
const SCOPE_PARSE_FAILED: &str = "invariant: tool required_scope must be a valid scope";

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
    pub(crate) input_schema: Arc<Map<String, Value>>,
    pub(crate) output_schema: Arc<Map<String, Value>>,
    pub(crate) required_scope: Scope,
}

// ---------------------------------------------------------------------------
// TOOLS
// ---------------------------------------------------------------------------

pub(crate) static TOOLS: &[ToolEntry] = &[
    ToolEntry {
        name: McpSetContextCall::NAME,
        title: McpSetContextCall::PRESENTATION.title,
        description: McpSetContextCall::PRESENTATION.description,
        input_schema: include_str!("schemas/set_context/input.json"),
        output_schema: include_str!("schemas/set_context/output.json"),
        required_scope: McpSetContextCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpIngestCall::NAME,
        title: McpIngestCall::PRESENTATION.title,
        description: McpIngestCall::PRESENTATION.description,
        input_schema: include_str!("schemas/ingest/input.json"),
        output_schema: include_str!("schemas/ingest/output.json"),
        required_scope: McpIngestCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpDiscoverCall::NAME,
        title: McpDiscoverCall::PRESENTATION.title,
        description: McpDiscoverCall::PRESENTATION.description,
        input_schema: include_str!("schemas/discover/input.json"),
        output_schema: include_str!("schemas/discover/output.json"),
        required_scope: McpDiscoverCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpExploreCall::NAME,
        title: McpExploreCall::PRESENTATION.title,
        description: McpExploreCall::PRESENTATION.description,
        input_schema: include_str!("schemas/explore/input.json"),
        output_schema: include_str!("schemas/explore/output.json"),
        required_scope: McpExploreCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpGetItemCall::NAME,
        title: McpGetItemCall::PRESENTATION.title,
        description: McpGetItemCall::PRESENTATION.description,
        input_schema: include_str!("schemas/get_item/input.json"),
        output_schema: include_str!("schemas/get_item/output.json"),
        required_scope: McpGetItemCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpFeedbackCall::NAME,
        title: McpFeedbackCall::PRESENTATION.title,
        description: McpFeedbackCall::PRESENTATION.description,
        input_schema: include_str!("schemas/feedback/input.json"),
        output_schema: include_str!("schemas/feedback/output.json"),
        required_scope: McpFeedbackCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpJobStatusCall::NAME,
        title: McpJobStatusCall::PRESENTATION.title,
        description: McpJobStatusCall::PRESENTATION.description,
        input_schema: include_str!("schemas/job_status/input.json"),
        output_schema: include_str!("schemas/job_status/output.json"),
        required_scope: McpJobStatusCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpReindexCall::NAME,
        title: McpReindexCall::PRESENTATION.title,
        description: McpReindexCall::PRESENTATION.description,
        input_schema: include_str!("schemas/reindex/input.json"),
        output_schema: include_str!("schemas/reindex/output.json"),
        required_scope: McpReindexCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpReindexCancelCall::NAME,
        title: McpReindexCancelCall::PRESENTATION.title,
        description: McpReindexCancelCall::PRESENTATION.description,
        input_schema: include_str!("schemas/reindex_cancel/input.json"),
        output_schema: include_str!("schemas/reindex_cancel/output.json"),
        required_scope: McpReindexCancelCall::REQUIRED_SCOPE,
    },
    ToolEntry {
        name: McpReindexPruneCall::NAME,
        title: McpReindexPruneCall::PRESENTATION.title,
        description: McpReindexPruneCall::PRESENTATION.description,
        input_schema: include_str!("schemas/reindex_prune/input.json"),
        output_schema: include_str!("schemas/reindex_prune/output.json"),
        required_scope: McpReindexPruneCall::REQUIRED_SCOPE,
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
/// Panics on first access if any embedded schema is not valid JSON or
/// any `required_scope` is not a valid scope string.
pub(crate) static PARSED_TOOLS: LazyLock<Vec<ParsedToolEntry>> = LazyLock::new(|| {
    TOOLS
        .iter()
        .map(|t| ParsedToolEntry {
            name: t.name,
            title: t.title,
            description: t.description,
            input_schema: parse_schema_object(t.input_schema, INPUT_SCHEMA_PARSE_FAILED),
            output_schema: parse_schema_object(t.output_schema, OUTPUT_SCHEMA_PARSE_FAILED),
            required_scope: Scope::parse(t.required_scope).unwrap_or_else(|e| {
                panic!(
                    "{SCOPE_PARSE_FAILED} for tool '{}' with required_scope '{}': {e}",
                    t.name, t.required_scope,
                )
            }),
        })
        .collect()
});

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Returns `(tool_name, required_scope)` pairs for every registered tool.
///
/// Tests use this to compute expected tool visibility from a set of
/// granted scopes, keeping assertions self-maintaining as tools are
/// added or removed.
#[cfg(feature = "test-helpers")]
pub fn tool_scope_registry() -> Vec<(&'static str, Scope)> {
    PARSED_TOOLS
        .iter()
        .map(|t| (t.name, t.required_scope.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_schema_object(json: &str, expect_msg: &str) -> Arc<Map<String, Value>> {
    let value: Value = serde_json::from_str(json).expect(expect_msg);
    Arc::new(value.as_object().expect(SCHEMA_MUST_BE_OBJECT).clone())
}

pub(crate) fn to_tool(entry: &ParsedToolEntry) -> Tool {
    Tool::new(
        entry.name,
        entry.description,
        Arc::clone(&entry.input_schema),
    )
    .with_title(entry.title)
    .with_raw_output_schema(Arc::clone(&entry.output_schema))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::Path};

    use super::*;
    use crate::contract::TOOL_NAME_PREFIX;

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
                    .strip_prefix(TOOL_NAME_PREFIX)
                    .unwrap_or_else(|| panic!("tool name must start with {TOOL_NAME_PREFIX}"))
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
                .strip_prefix(TOOL_NAME_PREFIX)
                .unwrap_or_else(|| panic!("tool name must start with {TOOL_NAME_PREFIX}"));

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
                    "input_schema": t.input_schema.as_ref(),
                    "output_schema": t.output_schema.as_ref(),
                    "required_scope": t.required_scope,
                })
            })
            .collect();

        tribal_test_utils::assert_json_snapshot!(&tools, "src/schemas/golden_snapshot.json");
    }

    #[test]
    fn test_scope_visibility_snapshot() {
        let visibility: Vec<Value> = PARSED_TOOLS
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t.name,
                    "required_scope": t.required_scope,
                })
            })
            .collect();

        tribal_test_utils::assert_json_snapshot!(
            &visibility,
            "src/schemas/scope_visibility_snapshot.json"
        );
    }
}
