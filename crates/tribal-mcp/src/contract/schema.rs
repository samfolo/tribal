//! Schema projection of the typed registry: the advertised per-tool
//! schema pair and the first-party client contract, rendered from the
//! declarations so the generator binary and the drift test share one
//! source.

use std::collections::BTreeMap;

use schemars::{schema::RootSchema, schema_for};
use serde::Serialize;

use super::{
    FirstPartyMcpContract, McpResourceRead, McpToolCall, TOOL_NAME_PREFIX,
    declarations::{
        McpDiscoverCall, McpExploreCall, McpFeedbackCall, McpGetItemCall, McpIngestCall,
        McpJobStatusCall, McpReindexCall, McpReindexCancelCall, McpReindexPruneCall,
        McpSetContextCall,
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
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FirstPartySchemaParts {
    tools: Vec<ClientTool>,
    resources: Vec<ClientResource>,
    required_scopes: Vec<&'static str>,
    definitions: BTreeMap<String, RootSchema>,
}

impl FirstPartySchemaParts {
    /// Adds one selected tool: its identity, presentation, and the
    /// definitions its request and response schemas file under.
    pub fn add_tool<C>(&mut self)
    where
        C: McpToolCall,
        C::Request: schemars::JsonSchema,
        C::Response: schemars::JsonSchema,
    {
        let input = <C::Request as schemars::JsonSchema>::schema_name();
        let output = <C::Response as schemars::JsonSchema>::schema_name();
        self.definitions
            .insert(input.clone(), schema_for!(C::Request));
        self.definitions
            .insert(output.clone(), schema_for!(C::Response));
        self.tools.push(ClientTool {
            name: C::NAME,
            required_scope: C::REQUIRED_SCOPE,
            title: C::PRESENTATION.title,
            description: C::PRESENTATION.description,
            input,
            output,
        });
    }

    /// Adds one selected resource and the definition its response files
    /// under.
    pub fn add_resource<R>(&mut self)
    where
        R: McpResourceRead,
        R::Response: schemars::JsonSchema,
    {
        let response = <R::Response as schemars::JsonSchema>::schema_name();
        self.definitions
            .insert(response.clone(), schema_for!(R::Response));
        self.resources.push(ClientResource {
            uri_template: R::URI_TEMPLATE,
            required_scope: R::REQUIRED_SCOPE,
            name: R::PRESENTATION.name,
            title: R::PRESENTATION.title,
            description: R::PRESENTATION.description,
            mime_type: R::PRESENTATION.mime_type,
            response,
        });
    }

    /// Names of the projected tools, for the selection-divergence gate.
    #[must_use]
    pub fn tool_names(&self) -> Vec<&'static str> {
        self.tools.iter().map(|tool| tool.name).collect()
    }

    /// Templates of the projected resources, for the same gate.
    #[must_use]
    pub fn resource_templates(&self) -> Vec<&'static str> {
        self.resources.iter().map(|r| r.uri_template).collect()
    }
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
    input: String,
    output: String,
}

/// One first-party resource: its identity and the definition name its
/// response schema is filed under.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientResource {
    uri_template: &'static str,
    required_scope: &'static str,
    name: &'static str,
    title: &'static str,
    description: &'static str,
    mime_type: &'static str,
    response: String,
}

/// Renders the first-party client contract document from the macro's
/// own projection — the selection has no second copy to drift from.
#[must_use]
pub fn client_contract() -> String {
    let mut parts = FirstPartyMcpContract::schema_parts();
    parts.required_scopes = FirstPartyMcpContract::required_scopes();
    render(&parts)
}

/// The one rendering every committed schema document uses.
fn render(value: &impl Serialize) -> String {
    let json = serde_json::to_string_pretty(value).expect("schema value serialises");
    format!("{json}\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{contract::TOOL_NAME_PREFIX, tools::TOOLS};

    #[test]
    fn test_the_schema_list_covers_the_advertised_table_exactly() {
        let schema_directories: Vec<&str> = advertised_tool_schemas()
            .iter()
            .map(|tool| tool.directory)
            .collect();
        let advertised: Vec<&str> = TOOLS
            .iter()
            .map(|tool| {
                tool.name
                    .strip_prefix(TOOL_NAME_PREFIX)
                    .expect("registered tool names carry the wire prefix")
            })
            .collect();

        assert_eq!(schema_directories, advertised);
    }
}
