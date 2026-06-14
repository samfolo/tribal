//! Tool failure vocabulary: the two-class routing contract.
//!
//! A tool failure is either model-recoverable (returned in-band as an
//! error-shaped tool result the model can act on) or a system failure
//! (routed to the stage-error path, never the conversation). The split is
//! structural: the recoverable variants name the model-addressable causes
//! and carry what an actionable diagnostic needs, so a transport fault
//! cannot be rendered at the model and an invalid-arguments report cannot
//! dead-letter a task. This module is the single classification point
//! every delivery and the deferred path consult.

use serde::{Deserialize, Serialize};

/// A tool execution failure, classified by construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolFailure {
    /// The model can recover: the diagnostic returns in-band as an
    /// error-shaped tool result and counts against the turn budget.
    Recoverable(RecoverableToolFailure),
    /// The system must recover: routed to task retry or suspension,
    /// exactly as stage errors are; the model never sees it.
    System {
        /// Operator-facing context for the stage-error path.
        context: String,
    },
}

/// The model-addressable failure causes.
///
/// Rendered diagnostics are hot-path, model-facing instructions that
/// decide whether the next turn recovers or flails; each variant carries
/// what its message needs to be actionable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoverableToolFailure {
    /// The named tool is not in the stage's registry.
    UnknownTool {
        /// The name the model called.
        name: String,
        /// The tools the stage actually offers.
        available: Vec<String>,
    },
    /// The arguments failed the tool's schema or semantic checks.
    InvalidArguments {
        /// The tool whose arguments failed.
        tool: String,
        /// What was wrong and what a valid call looks like.
        detail: String,
    },
    /// The call referenced something outside the stage's scope.
    OutOfScope {
        /// The tool that refused.
        tool: String,
        /// What was out of scope and what the scope is.
        detail: String,
    },
    /// The call succeeded but produced nothing where the tool expected
    /// to produce something actionable.
    EmptyResult {
        /// The tool that came back empty.
        tool: String,
        /// What was searched or read, so the model can reformulate.
        detail: String,
    },
}

impl RecoverableToolFailure {
    /// Renders the in-band diagnostic for the error-shaped tool result.
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            Self::UnknownTool { name, available } => format!(
                "No tool named '{name}' exists here. Available tools: {}.",
                available.join(", ")
            ),
            Self::InvalidArguments { tool, detail } => {
                format!("Invalid arguments for '{tool}': {detail}")
            }
            Self::OutOfScope { tool, detail } => {
                format!("'{tool}' refused an out-of-scope request: {detail}")
            }
            Self::EmptyResult { tool, detail } => {
                format!("'{tool}' found nothing: {detail}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_tool_renders_the_available_set() {
        let failure = RecoverableToolFailure::UnknownTool {
            name: "delete_item".to_owned(),
            available: vec![
                "search_similar_items".to_owned(),
                "submit_result".to_owned(),
            ],
        };
        let rendered = failure.render();
        assert!(rendered.contains("delete_item"));
        assert!(rendered.contains("search_similar_items, submit_result"));
    }

    #[test]
    fn test_invalid_arguments_renders_the_detail() {
        let failure = RecoverableToolFailure::InvalidArguments {
            tool: "search_similar_items".to_owned(),
            detail: "'query' is required and must be a non-empty string".to_owned(),
        };
        assert_eq!(
            failure.render(),
            "Invalid arguments for 'search_similar_items': 'query' is required and must be a \
             non-empty string"
        );
    }

    #[test]
    fn test_system_failures_are_not_renderable_in_band() {
        // The type encodes the routing split: only the recoverable half
        // has a render path, so a transport fault cannot reach the
        // conversation by construction.
        let failure = ToolFailure::System {
            context: "provider connection reset".to_owned(),
        };
        assert!(matches!(failure, ToolFailure::System { .. }));
    }
}
