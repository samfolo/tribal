//! Schema projection of the typed registry: the advertised per-tool
//! schema pair and the first-party client contract, rendered from the
//! declarations so the generator binary and the drift test share one
//! source.

use std::collections::BTreeMap;

use schemars::{schema::RootSchema, schema_for};
use serde::Serialize;
use tribal_wire::mcp::{
    McpIngestRequest, McpIngestResponse, McpIngestionInputResponse, McpJobStatusRequest,
    McpJobStatusResponse, McpRecentIngestionsResponse,
};

use super::{
    FirstPartyMcpContract, McpResourceRead, McpToolCall, TOOL_NAME_PREFIX,
    declarations::{
        McpDiscoverCall, McpExploreCall, McpFeedbackCall, McpGetItemCall, McpIngestCall,
        McpIngestionInputResource, McpJobStatusCall, McpRecentIngestionsResource, McpReindexCall,
        McpReindexCancelCall, McpReindexPruneCall, McpSetContextCall,
    },
};

/// One tool's rendered schema pair: its schema-directory basename and
/// the input/output documents exactly as committed.
pub struct RenderedToolSchemas {
    /// Directory basename under `src/schemas/`.
    pub directory: &'static str,
    /// The input schema document.
    pub input: String,
    /// The output schema document.
    pub output: String,
}

/// Renders every registered tool's advertised schema pair.
#[must_use]
pub fn advertised_tool_schemas() -> Vec<RenderedToolSchemas> {
    vec![
        tool_schemas::<McpSetContextCall>(),
        tool_schemas::<McpIngestCall>(),
        tool_schemas::<McpDiscoverCall>(),
        tool_schemas::<McpExploreCall>(),
        tool_schemas::<McpGetItemCall>(),
        tool_schemas::<McpFeedbackCall>(),
        tool_schemas::<McpJobStatusCall>(),
        tool_schemas::<McpReindexCall>(),
        tool_schemas::<McpReindexCancelCall>(),
        tool_schemas::<McpReindexPruneCall>(),
    ]
}

/// One registered tool's schema directory (its `NAME` with the wire
/// prefix stripped) and rendered input/output documents.
fn tool_schemas<C>() -> RenderedToolSchemas
where
    C: McpToolCall,
    C::Request: schemars::JsonSchema,
    C::Response: schemars::JsonSchema,
{
    let directory = C::NAME.strip_prefix(TOOL_NAME_PREFIX).unwrap_or_else(|| {
        panic!(
            "tool name `{}` must start with `{TOOL_NAME_PREFIX}`",
            C::NAME
        )
    });
    RenderedToolSchemas {
        directory,
        input: render(&schema_for!(C::Request)),
        output: render(&schema_for!(C::Response)),
    }
}

// ---------------------------------------------------------------------------
// First-party client contract
// ---------------------------------------------------------------------------

/// The first-party MCP contract native clients speak: the selected tools
/// and resources, the union of scopes they require, and the shared schema
/// definitions their input/output/response fields name.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientContract {
    tools: Vec<ClientTool>,
    resources: Vec<ClientResource>,
    required_scopes: Vec<&'static str>,
    definitions: BTreeMap<String, RootSchema>,
}

/// One first-party tool: its identity, presentation, and the definition
/// names its input/output schemas are filed under.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientTool {
    name: &'static str,
    required_scope: &'static str,
    title: &'static str,
    description: &'static str,
    input: &'static str,
    output: &'static str,
}

/// One first-party resource: its identity and the definition name its
/// response schema is filed under.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientResource {
    uri_template: &'static str,
    required_scope: &'static str,
    response: &'static str,
}

/// Renders the first-party client contract document.
#[must_use]
pub fn client_contract() -> String {
    let mut definitions = BTreeMap::new();
    for (name, schema) in [
        ("McpIngestRequest", schema_for!(McpIngestRequest)),
        ("McpIngestResponse", schema_for!(McpIngestResponse)),
        ("McpJobStatusRequest", schema_for!(McpJobStatusRequest)),
        ("McpJobStatusResponse", schema_for!(McpJobStatusResponse)),
        (
            "McpRecentIngestionsResponse",
            schema_for!(McpRecentIngestionsResponse),
        ),
        (
            "McpIngestionInputResponse",
            schema_for!(McpIngestionInputResponse),
        ),
    ] {
        definitions.insert(name.to_owned(), schema);
    }

    let contract = ClientContract {
        tools: vec![
            ClientTool {
                name: McpIngestCall::NAME,
                required_scope: McpIngestCall::REQUIRED_SCOPE,
                title: McpIngestCall::PRESENTATION.title,
                description: McpIngestCall::PRESENTATION.description,
                input: "McpIngestRequest",
                output: "McpIngestResponse",
            },
            ClientTool {
                name: McpJobStatusCall::NAME,
                required_scope: McpJobStatusCall::REQUIRED_SCOPE,
                title: McpJobStatusCall::PRESENTATION.title,
                description: McpJobStatusCall::PRESENTATION.description,
                input: "McpJobStatusRequest",
                output: "McpJobStatusResponse",
            },
        ],
        resources: vec![
            ClientResource {
                uri_template: McpRecentIngestionsResource::URI_TEMPLATE,
                required_scope: McpRecentIngestionsResource::REQUIRED_SCOPE,
                response: "McpRecentIngestionsResponse",
            },
            ClientResource {
                uri_template: McpIngestionInputResource::URI_TEMPLATE,
                required_scope: McpIngestionInputResource::REQUIRED_SCOPE,
                response: "McpIngestionInputResponse",
            },
        ],
        required_scopes: FirstPartyMcpContract::required_scopes(),
        definitions,
    };
    render(&contract)
}

/// The one rendering every committed schema document uses.
fn render(value: &impl Serialize) -> String {
    let json = serde_json::to_string_pretty(value).expect("schema value serialises");
    format!("{json}\n")
}
