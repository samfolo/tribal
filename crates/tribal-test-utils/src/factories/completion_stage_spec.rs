use tribal_domain::{ProviderKind, StageParameters};
use tribal_inference::CompletionStageSpec;

/// A completion stage spec carrying the workspace's Ollama defaults, the one
/// place tests share rather than re-deriving the shape. Override a field by
/// struct update: `CompletionStageSpec { model, ..a_completion_stage_spec() }`.
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
