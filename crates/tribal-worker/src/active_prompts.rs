//! The active-prompts seam: how the worker reads agentic prompt slots.
//!
//! The active set lives with the MCP server state and is mutated by the
//! hot-reload watcher; the worker resolves loop prompts at claim through
//! this trait, so both consumers read one hot-reload-coherent source and
//! no second prompt-resolution mechanism exists. The dependency points
//! inward: the owner of the state implements the trait and injects it at
//! orchestration.

use async_trait::async_trait;
use tribal_domain::{PromptClass, PromptRole, PromptStage, PromptVersionId};

/// Read access to the active agentic prompt slots.
#[async_trait]
pub trait ActiveAgenticPrompts: Send + Sync {
    /// The active version id for an agentic prompt slot, when one is
    /// live.
    async fn version_id(
        &self,
        stage: PromptStage,
        class: PromptClass,
        role: PromptRole,
    ) -> Option<PromptVersionId>;
}

/// The inert source: no agentic slot is ever live. For executions with
/// no agentic prompt state behind them — tests, and harnesses that never
/// select a loop executor.
pub struct NoAgenticPrompts;

#[async_trait]
impl ActiveAgenticPrompts for NoAgenticPrompts {
    async fn version_id(
        &self,
        _stage: PromptStage,
        _class: PromptClass,
        _role: PromptRole,
    ) -> Option<PromptVersionId> {
        None
    }
}
