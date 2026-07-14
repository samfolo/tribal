//! MCP integration export DTOs.

use std::fmt;

use serde::{Deserialize, Serialize};
use tribal_domain::ProjectId;

use super::{AbsoluteDirectoryPath, ConfigRevision, Revisioned};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "context", content = "data", rename_all = "snake_case")]
pub enum StdioProjectContext {
    Unscoped,
    Project { selector: ProjectSelector },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "selection", content = "data", rename_all = "snake_case")]
pub enum McpTargetSelection {
    Configured { policy: ConfiguredMcpTarget },
    Explicit { target: McpTarget },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "exposure", content = "data", rename_all = "snake_case")]
pub enum ConfiguredMcpTarget {
    Public { stdio_context: StdioProjectContext },
    ExportPersistedBearer { stdio_context: StdioProjectContext },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "transport", content = "data", rename_all = "snake_case")]
pub enum McpTarget {
    Stdio { context: StdioProjectContext },
    Http { auth: NetworkIntegrationAuth },
    Sse { auth: NetworkIntegrationAuth },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "source", content = "data", rename_all = "snake_case")]
pub enum ProjectSelector {
    Id { id: ProjectId },
    WorkingTree { directory: AbsoluteDirectoryPath },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum NetworkIntegrationAuth {
    OAuth,
    ExportPersistedBearer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PublicMcpConfigDocument {
    pub server_name: String,
    pub entry: PublicMcpServerEntry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "transport", content = "data", rename_all = "snake_case")]
pub enum PublicMcpServerEntry {
    Stdio { command: String, args: Vec<String> },
    Http { url: String },
    Sse { url: String },
}

/// Complete bearer-bearing MCP document revealed only within a closure.
#[derive(PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SensitiveMcpConfigDocument(serde_json::Value);

impl SensitiveMcpConfigDocument {
    #[must_use]
    pub fn new(document: serde_json::Value) -> Self {
        Self(document)
    }

    pub fn with_document<T>(&self, body: impl FnOnce(&serde_json::Value) -> T) -> T {
        body(&self.0)
    }
}

impl fmt::Debug for SensitiveMcpConfigDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted MCP configuration document>")
    }
}

impl fmt::Display for SensitiveMcpConfigDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for SensitiveMcpConfigDocument {
    fn schema_name() -> String {
        "SensitiveMcpConfigDocument".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = generator.subschema_for::<serde_json::Value>().into_object();
        schema.extensions.insert(
            "x-cortex-swift-type".to_owned(),
            serde_json::Value::String("scoped-redacted-json".to_owned()),
        );
        schema.into()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "exposure", content = "data", rename_all = "snake_case")]
pub enum McpConfigEntry {
    Public {
        document: PublicMcpConfigDocument,
    },
    PersistedBearer {
        document: SensitiveMcpConfigDocument,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct McpConfigRequest {
    pub expected_revision: ConfigRevision,
    pub target: McpTargetSelection,
}

pub type McpConfigResult = Revisioned<McpConfigEntry>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sensitive_document_projections_are_redacted() {
        let document = SensitiveMcpConfigDocument::new(serde_json::json!({
            "headers": { "Authorization": "Bearer sentinel-secret" }
        }));
        assert!(!format!("{document:?}").contains("sentinel"));
        assert!(!document.to_string().contains("sentinel"));
        assert!(document.with_document(|value| value.to_string().contains("sentinel")));
    }
}
