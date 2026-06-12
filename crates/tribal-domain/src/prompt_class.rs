//! Prompt class classification.
//!
//! Identifies which executor surface a prompt version serves. The class
//! is orthogonal to [`PromptRole`](crate::PromptRole): every class
//! carries the same system/user pair, so a new class never multiplies
//! the role vocabulary and a new role never multiplies the classes.
//! Which (stage, class) pairings exist is the template vocabulary's
//! fact, owned in one place by its enumeration, never by match arms.

use serde::{Deserialize, Serialize};

/// The executor surface a prompt version serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptClass {
    /// The launched single-call execution.
    OneShot,
    /// The agentic turn loop.
    Loop,
    /// The verifier child reviewing an agentic submission.
    Verifier,
}

enum_text_conversions!(PromptClass {
    PromptClass::OneShot => "one_shot",
    PromptClass::Loop => "loop",
    PromptClass::Verifier => "verifier",
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_prompt_class_serde_roundtrip, PromptClass {
        PromptClass::OneShot => "one_shot",
        PromptClass::Loop => "loop",
        PromptClass::Verifier => "verifier",
    });

    enum_text_tests!(test_prompt_class_text_roundtrip, PromptClass {
        PromptClass::OneShot => "one_shot",
        PromptClass::Loop => "loop",
        PromptClass::Verifier => "verifier",
    });
}
