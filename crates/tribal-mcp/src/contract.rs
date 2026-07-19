//! The typed MCP contract registry: tool and resource identity declared
//! once, in Rust, as the sole association of wire name, request and
//! response types, required scope, and presentation copy.
//!
//! The first-party declaration selects which calls and resources are
//! generated for native clients; it never redefines them and it is not a
//! runtime visibility flag.

pub mod declarations;
#[cfg(feature = "schema")]
pub mod schema;

use serde::{Serialize, de::DeserializeOwned};

// ---------------------------------------------------------------------------
// Presentation
// ---------------------------------------------------------------------------

/// The model- and human-facing copy a tool carries, held on its
/// declaration so it exists exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolPresentation {
    /// Display title.
    pub title: &'static str,
    /// The tool's full description.
    pub description: &'static str,
}

// ---------------------------------------------------------------------------
// Naming convention
// ---------------------------------------------------------------------------

/// The wire-name prefix every registered MCP tool carries; stripping it
/// yields the tool's schema-directory basename.
pub const TOOL_NAME_PREFIX: &str = "tribal_";

// ---------------------------------------------------------------------------
// Declarations
// ---------------------------------------------------------------------------

/// One MCP tool, typed: the wire name, the scope its dispatch requires,
/// its presentation, and the request/response pair its schema derives
/// from.
pub trait McpToolCall {
    const NAME: &'static str;
    const REQUIRED_SCOPE: &'static str;
    const PRESENTATION: ToolPresentation;
    type Request: Serialize + DeserializeOwned;
    type Response: Serialize + DeserializeOwned;
}

/// One MCP resource read, typed: the URI template, the scope its read
/// requires, and the response its schema derives from.
pub trait McpResourceRead {
    const URI_TEMPLATE: &'static str;
    const REQUIRED_SCOPE: &'static str;
    type Response: Serialize + DeserializeOwned;
}

// ---------------------------------------------------------------------------
// First-party selection
// ---------------------------------------------------------------------------

/// Declares the first-party contract: the tools and resources generated
/// for native clients, selected from the registry's declarations.
///
/// Expands to `FirstPartyMcpContract`, whose associated functions are
/// the one enumeration the contract generator reads: each tool's name,
/// scope, and schema types; each resource's template and scope; and the
/// sorted, deduplicated union of required scopes.
#[macro_export]
macro_rules! declare_first_party_mcp_contract {
    (tools: [$($tool:ty),+ $(,)?]; resources: [$($resource:ty),+ $(,)?];) => {
        /// The first-party MCP contract selection.
        pub struct FirstPartyMcpContract;

        impl FirstPartyMcpContract {
            /// Wire names of the selected tools.
            #[must_use]
            pub fn tool_names() -> Vec<&'static str> {
                vec![$(<$tool as $crate::contract::McpToolCall>::NAME),+]
            }

            /// URI templates of the selected resources.
            #[must_use]
            pub fn resource_templates() -> Vec<&'static str> {
                vec![$(<$resource as $crate::contract::McpResourceRead>::URI_TEMPLATE),+]
            }

            /// The sorted, deduplicated union of the selection's
            /// required scopes.
            #[must_use]
            pub fn required_scopes() -> Vec<&'static str> {
                let mut scopes = vec![
                    $(<$tool as $crate::contract::McpToolCall>::REQUIRED_SCOPE,)+
                    $(<$resource as $crate::contract::McpResourceRead>::REQUIRED_SCOPE,)+
                ];
                scopes.sort_unstable();
                scopes.dedup();
                scopes
            }
        }

        /// The schema projection of this exact selection — built from the
        /// same list, so the generated contract has no second copy of the
        /// selection to drift from.
        #[cfg(feature = "schema")]
        impl FirstPartyMcpContract {
            #[must_use]
            pub fn schema_parts() -> $crate::contract::schema::FirstPartySchemaParts {
                let mut parts = $crate::contract::schema::FirstPartySchemaParts::default();
                $(parts.add_tool::<$tool>();)+
                $(parts.add_resource::<$resource>();)+
                parts
            }
        }
    };
}

// ---------------------------------------------------------------------------
// The registered first-party contract
// ---------------------------------------------------------------------------

use declarations::{
    McpIngestCall, McpIngestionInputResource, McpJobStatusCall, McpRecentIngestionsResource,
};

declare_first_party_mcp_contract! {
    tools: [McpIngestCall, McpJobStatusCall];
    resources: [McpRecentIngestionsResource, McpIngestionInputResource];
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlphaCall;
    impl McpToolCall for AlphaCall {
        const NAME: &'static str = "alpha";
        const REQUIRED_SCOPE: &'static str = "tribal.knowledge:write";
        const PRESENTATION: ToolPresentation = ToolPresentation {
            title: "Alpha",
            description: "does alpha",
        };
        type Request = serde_json::Value;
        type Response = serde_json::Value;
    }

    struct BetaRead;
    impl McpResourceRead for BetaRead {
        const URI_TEMPLATE: &'static str = "tribal://beta/{id}";
        const REQUIRED_SCOPE: &'static str = "tribal.knowledge:read";
        type Response = serde_json::Value;
    }

    struct GammaRead;
    impl McpResourceRead for GammaRead {
        const URI_TEMPLATE: &'static str = "tribal://gamma";
        const REQUIRED_SCOPE: &'static str = "tribal.knowledge:write";
        type Response = serde_json::Value;
    }

    declare_first_party_mcp_contract! {
        tools: [AlphaCall];
        resources: [BetaRead, GammaRead];
    }

    #[test]
    fn test_the_selection_enumerates_its_declarations() {
        assert_eq!(FirstPartyMcpContract::tool_names(), vec!["alpha"]);
        assert_eq!(
            FirstPartyMcpContract::resource_templates(),
            vec!["tribal://beta/{id}", "tribal://gamma"],
        );
    }

    #[cfg(feature = "schema")]
    #[test]
    fn test_the_schema_projection_is_built_from_the_selection_itself() {
        let parts = FirstPartyMcpContract::schema_parts();

        assert_eq!(parts.tool_names(), FirstPartyMcpContract::tool_names());
        assert_eq!(
            parts.resource_templates(),
            FirstPartyMcpContract::resource_templates(),
        );
    }

    #[test]
    fn test_required_scopes_are_the_sorted_deduplicated_union() {
        assert_eq!(
            FirstPartyMcpContract::required_scopes(),
            vec!["tribal.knowledge:read", "tribal.knowledge:write"],
        );
    }
}
