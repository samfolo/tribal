//! Prompt role classification.
//!
//! Identifies whether a prompt version is a system prompt (static
//! instructions + schema) or a user prompt (dynamic per-request data).

use serde::{Deserialize, Serialize};

/// The role a prompt version serves in the completion request.
///
/// System prompts contain cacheable instructions and schema; user
/// prompts contain per-request dynamic data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptRole {
    /// The system prompt — static instructions and schema.
    System,
    /// The user prompt — dynamic per-request data.
    User,
}

enum_text_conversions!(PromptRole {
    PromptRole::System => "system",
    PromptRole::User => "user",
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_prompt_role_serde_roundtrip, PromptRole {
        PromptRole::System => "system",
        PromptRole::User => "user",
    });

    enum_text_tests!(test_prompt_role_text_roundtrip, PromptRole {
        PromptRole::System => "system",
        PromptRole::User => "user",
    });
}
