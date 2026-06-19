//! The inward tool registry: how a stage's loop offers and executes
//! tools.
//!
//! Tools are named, schema-typed, stage-scoped semantic operations. The
//! registry owns the model-facing contract mechanics (name lookup with
//! actionable diagnostics, response-size trimming, and the wire
//! projection) while graph semantics live entirely in the tool
//! implementations the worker registers (the runtime never learns them).
//! Scoping is structural: a tool captures its project at construction,
//! so no argument the model supplies can widen it.
//!
//! Execution is two-phase, the no-transaction-across-inference rule
//! applied to tools: provider-touching work (an embedding call) runs in
//! the prepare phase with no transaction held, and the fenced phase
//! commits the result record plus any internal side effect against the
//! caller's transaction.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::{RecoverableToolFailure, ToolDescriptor, ToolExecutionMode, ToolFailure};

/// The marker appended to a result trimmed to its declared bound.
const TRIM_MARKER_PREFIX: &str = "\n[truncated: over the ";

/// A successful tool execution's product.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolOutcome {
    /// The serialised result content, before the registry's trim.
    pub content: String,
}

/// One stage-scoped tool behind the registry.
///
/// Implementations capture their scope (the project, the job, the
/// thread) at construction and treat the model-supplied arguments as
/// untrusted input: argument faults are recoverable failures, never
/// panics.
#[async_trait]
pub trait StageTool: Send + Sync {
    /// The tool's contract: name, description, schema, size bound,
    /// safety tier, and execution mode.
    fn descriptor(&self) -> &ToolDescriptor;

    /// Phase one: provider-touching work with no transaction held.
    ///
    /// The default is the no-op for the majority of tools, whose work is
    /// purely a fenced read. A tool that must call a provider (the
    /// search tool's query embedding) does so here and returns the
    /// prepared state the fenced phase consumes.
    async fn prepare(
        &self,
        arguments: &serde_json::Value,
    ) -> Result<serde_json::Value, ToolFailure> {
        let _ = arguments;
        Ok(serde_json::Value::Null)
    }

    /// Phase two: the fenced execution against the caller's transaction.
    ///
    /// Internal-transactional tools commit their side effect here, in
    /// the same transaction that commits the tool-result record; pure
    /// tools only read.
    async fn execute(
        &self,
        conn: &mut PgConnection,
        prepared: serde_json::Value,
        arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure>;
}

/// A tool could not join the registry.
///
/// Registration failures are wiring bugs, surfaced loudly at boot
/// rather than discovered when the model calls.
#[derive(Debug, thiserror::Error)]
pub enum ToolRegistryError {
    /// Two tools claimed one name.
    #[error("a tool named '{name}' is already registered")]
    DuplicateName {
        /// The contested name.
        name: String,
    },
    /// The deferred execution mode has no loop-side dispatch path.
    #[error("tool '{name}' declares deferred execution, which no dispatch path serves")]
    DeferredUnsupported {
        /// The refused tool.
        name: String,
    },
    /// The external safety tier needs intent rows, which no intent store
    /// serves.
    #[error("tool '{name}' declares the external safety tier, which no intent store serves")]
    ExternalTierUnsupported {
        /// The refused tool.
        name: String,
    },
}

/// One stage's tool set.
#[derive(Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn StageTool>>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a tool, refusing contracts the runtime cannot honour.
    ///
    /// # Errors
    ///
    /// Returns [`ToolRegistryError`] on a duplicate name, a deferred
    /// execution mode, or the external safety tier.
    pub fn register(&mut self, tool: Arc<dyn StageTool>) -> Result<(), ToolRegistryError> {
        let descriptor = tool.descriptor();
        if descriptor.execution_mode == ToolExecutionMode::Deferred {
            return Err(ToolRegistryError::DeferredUnsupported {
                name: descriptor.name.clone(),
            });
        }
        if descriptor.safety_tier == tribal_domain::ToolSafetyTier::ExternalWithIntent {
            return Err(ToolRegistryError::ExternalTierUnsupported {
                name: descriptor.name.clone(),
            });
        }
        if self.tools.contains_key(&descriptor.name) {
            return Err(ToolRegistryError::DuplicateName {
                name: descriptor.name.clone(),
            });
        }
        self.tools.insert(descriptor.name.clone(), tool);
        Ok(())
    }

    /// Looks a tool up by the name the model called.
    ///
    /// # Errors
    ///
    /// Returns [`RecoverableToolFailure::UnknownTool`] carrying the
    /// stage's actual tool names, so the diagnostic is actionable.
    pub fn lookup(&self, name: &str) -> Result<&Arc<dyn StageTool>, RecoverableToolFailure> {
        self.tools
            .get(name)
            .ok_or_else(|| RecoverableToolFailure::UnknownTool {
                name: name.to_owned(),
                available: self.tools.keys().cloned().collect(),
            })
    }

    /// The registered descriptors, in name order: the binding-hash
    /// input.
    #[must_use]
    pub fn descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools
            .values()
            .map(|tool| tool.descriptor().clone())
            .collect()
    }

    /// Trims a result to its tool's declared bound, appending the explicit
    /// marker when anything was cut. The marker's room is reserved inside
    /// the bound, so the model-facing result never exceeds the declared
    /// ceiling; trimming respects UTF-8 boundaries, so the kept prefix is
    /// always valid text. A bound too small to hold even the marker is
    /// degenerate; the marker alone, clamped to the bound, is the
    /// deterministic result there.
    #[must_use]
    pub fn trim_to_bound(descriptor: &ToolDescriptor, content: String) -> String {
        let bound = descriptor.response_size_bound as usize;
        if content.len() <= bound {
            return content;
        }
        let marker = format!("{TRIM_MARKER_PREFIX}{bound}-byte limit]");
        if marker.len() >= bound {
            let mut end = bound;
            while end > 0 && !marker.is_char_boundary(end) {
                end -= 1;
            }
            return marker[..end].to_owned();
        }
        let mut cut = bound - marker.len();
        while cut > 0 && !content.is_char_boundary(cut) {
            cut -= 1;
        }
        let mut trimmed = content[..cut].to_owned();
        trimmed.push_str(&marker);
        trimmed
    }

    /// Bounds a result to its tool's declared size, returning the output and
    /// whether it overflowed, so the caller records an overflow as a
    /// model-recoverable failure, never a silent truncated success.
    #[must_use]
    pub fn bound_result(descriptor: &ToolDescriptor, content: String) -> (String, bool) {
        let oversized = content.len() > descriptor.response_size_bound as usize;
        (Self::trim_to_bound(descriptor, content), oversized)
    }
}

#[cfg(test)]
mod tests {
    use tribal_domain::ToolSafetyTier;

    use super::*;

    fn a_descriptor(name: &str, mode: ToolExecutionMode, tier: ToolSafetyTier) -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(name.to_owned())
            .description("a test tool".to_owned())
            .input_schema(serde_json::json!({"type": "object"}))
            .response_size_bound(64)
            .safety_tier(tier)
            .execution_mode(mode)
            .build()
    }

    struct EchoTool {
        descriptor: ToolDescriptor,
    }

    #[async_trait]
    impl StageTool for EchoTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }

        async fn execute(
            &self,
            _conn: &mut PgConnection,
            _prepared: serde_json::Value,
            arguments: &serde_json::Value,
        ) -> Result<ToolOutcome, ToolFailure> {
            Ok(ToolOutcome {
                content: arguments.to_string(),
            })
        }
    }

    fn echo(name: &str, mode: ToolExecutionMode, tier: ToolSafetyTier) -> Arc<dyn StageTool> {
        Arc::new(EchoTool {
            descriptor: a_descriptor(name, mode, tier),
        })
    }

    #[test]
    fn test_register_rejects_the_deferred_execution_mode() {
        let mut registry = ToolRegistry::new();
        let err = registry
            .register(echo(
                "slow",
                ToolExecutionMode::Deferred,
                ToolSafetyTier::Pure,
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            ToolRegistryError::DeferredUnsupported { ref name } if name == "slow"
        ));
    }

    #[test]
    fn test_register_rejects_the_external_safety_tier() {
        let mut registry = ToolRegistry::new();
        let err = registry
            .register(echo(
                "webhook",
                ToolExecutionMode::Immediate,
                ToolSafetyTier::ExternalWithIntent,
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            ToolRegistryError::ExternalTierUnsupported { ref name } if name == "webhook"
        ));
    }

    #[test]
    fn test_register_rejects_duplicate_names() {
        let mut registry = ToolRegistry::new();
        registry
            .register(echo(
                "search",
                ToolExecutionMode::Immediate,
                ToolSafetyTier::Pure,
            ))
            .expect("first registration");
        let err = registry
            .register(echo(
                "search",
                ToolExecutionMode::Immediate,
                ToolSafetyTier::Pure,
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            ToolRegistryError::DuplicateName { ref name } if name == "search"
        ));
    }

    #[test]
    fn test_lookup_miss_names_the_available_tools() {
        let mut registry = ToolRegistry::new();
        registry
            .register(echo(
                "search",
                ToolExecutionMode::Immediate,
                ToolSafetyTier::Pure,
            ))
            .expect("registration");
        let Err(err) = registry.lookup("delete_everything") else {
            panic!("an unknown name must miss");
        };
        assert!(matches!(
            err,
            RecoverableToolFailure::UnknownTool { ref name, ref available }
                if name == "delete_everything" && available == &vec!["search".to_owned()]
        ));
    }

    #[test]
    fn test_trim_appends_the_marker_within_the_bound() {
        let descriptor = a_descriptor("search", ToolExecutionMode::Immediate, ToolSafetyTier::Pure);
        let bound = descriptor.response_size_bound as usize;

        let short = ToolRegistry::trim_to_bound(&descriptor, "short".to_owned());
        assert_eq!(short, "short");

        let long = ToolRegistry::trim_to_bound(&descriptor, "x".repeat(100));
        assert!(long.starts_with('x'), "the payload prefix is kept");
        assert!(long.contains("over the 64-byte limit"));
        assert!(
            long.len() <= bound,
            "the marker's room is reserved inside the bound: {} > {bound}",
            long.len(),
        );
    }

    #[test]
    fn test_bound_result_flags_only_an_overflow() {
        let descriptor = a_descriptor("search", ToolExecutionMode::Immediate, ToolSafetyTier::Pure);

        let (output, oversized) = ToolRegistry::bound_result(&descriptor, "short".to_owned());
        assert_eq!(output, "short");
        assert!(!oversized, "a result within bound is whole and not flagged");

        let (output, oversized) = ToolRegistry::bound_result(&descriptor, "x".repeat(100));
        assert!(
            oversized,
            "an overflow is flagged so the caller marks it an error"
        );
        assert!(output.contains("over the 64-byte limit"));
        assert!(output.len() <= descriptor.response_size_bound as usize);
    }

    #[test]
    fn test_trim_respects_utf8_boundaries() {
        let descriptor = a_descriptor("search", ToolExecutionMode::Immediate, ToolSafetyTier::Pure);
        // Each '£' is two bytes; the payload budget (the bound less the
        // marker) can fall mid-character, so the cut backs up to the
        // boundary rather than splitting it.
        let trimmed = ToolRegistry::trim_to_bound(&descriptor, "£".repeat(40));
        assert!(trimmed.contains("over the 64-byte limit"));
        assert!(!trimmed.starts_with('\u{fffd}'));
        assert!(trimmed.len() <= descriptor.response_size_bound as usize);
    }

    #[test]
    fn test_trim_clamps_when_the_bound_cannot_hold_the_marker() {
        // A bound smaller than the marker is degenerate; the result is the
        // marker clamped to the bound, never exceeding it.
        let descriptor = ToolDescriptor::builder()
            .name("search".to_owned())
            .description("a test tool".to_owned())
            .input_schema(serde_json::json!({"type": "object"}))
            .response_size_bound(8)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build();
        let trimmed = ToolRegistry::trim_to_bound(&descriptor, "x".repeat(100));
        assert!(
            trimmed.len() <= 8,
            "the result must not exceed the bound, got {}",
            trimmed.len(),
        );
    }
}
