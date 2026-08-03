//! Fixed-value test fixtures, distinct from the builder factories.

use tribal_domain::{ProviderKind, StageParameters};
use tribal_inference::CompletionStageSpec;

/// Tags whose bytewise (`C`) and linguistic (ICU `en-US`) orders disagree:
/// `C` sorts `Zebra` before every lowercase letter and both accented tags
/// last, while a linguistic collation interleaves them. Shared by the
/// collation-parity fixtures and their raw-ordering self-guard.
pub const LOCALE_SENSITIVE_TAGS: [&str; 5] = ["zebra", "Zebra", "apple", "Ápple", "émigré"];

/// A completion stage spec on the workspace's Ollama defaults, shared so tests
/// do not re-derive the shape. Override a field by struct update:
/// `CompletionStageSpec { model, ..a_completion_stage_spec() }`.
#[must_use]
pub fn a_completion_stage_spec() -> CompletionStageSpec {
    CompletionStageSpec {
        provider: ProviderKind::Ollama,
        model: "llama3".to_owned(),
        base_url: "http://localhost:11434".to_owned(),
        api_key: String::new(),
        parameters: StageParameters::default(),
    }
}
