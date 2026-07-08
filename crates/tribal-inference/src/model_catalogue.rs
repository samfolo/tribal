//! Static model descriptors for the operator-facing config facade.

use tribal_domain::ProviderKind;

/// One known model option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnownModel {
    /// Provider this row belongs to.
    pub provider: ProviderKind,
    /// Provider-native model identifier.
    pub model: &'static str,
    /// Operator-facing display name.
    pub display_name: &'static str,
}

const KNOWN_MODELS: &[KnownModel] = &[
    KnownModel {
        provider: ProviderKind::Ollama,
        model: "llama3.1:8b",
        display_name: "Llama 3.1 8B",
    },
    KnownModel {
        provider: ProviderKind::Ollama,
        model: "llama3.1:70b",
        display_name: "Llama 3.1 70B",
    },
    KnownModel {
        provider: ProviderKind::OpenAi,
        model: "gpt-4o-mini",
        display_name: "GPT-4o mini",
    },
    KnownModel {
        provider: ProviderKind::OpenAi,
        model: "gpt-5",
        display_name: "GPT-5",
    },
    KnownModel {
        provider: ProviderKind::Anthropic,
        model: "claude-haiku-4-5-20251001",
        display_name: "Claude Haiku 4.5",
    },
    KnownModel {
        provider: ProviderKind::Anthropic,
        model: "claude-opus-4-7",
        display_name: "Claude Opus 4.7",
    },
    KnownModel {
        provider: ProviderKind::Platform,
        model: "gpt-5",
        display_name: "GPT-5 (Platform)",
    },
    KnownModel {
        provider: ProviderKind::Platform,
        model: "claude-haiku-4-5-20251001",
        display_name: "Claude Haiku 4.5 (Platform)",
    },
];

/// Returns the static model catalogue in display order.
#[must_use]
pub fn known_models() -> &'static [KnownModel] {
    KNOWN_MODELS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalogue_includes_distinguishable_platform_rows() {
        let platform = known_models()
            .iter()
            .filter(|model| model.provider == ProviderKind::Platform)
            .collect::<Vec<_>>();
        assert!(
            !platform.is_empty(),
            "the catalogue must expose managed-platform choices"
        );
        assert!(
            platform
                .iter()
                .all(|model| model.display_name.contains("Platform")),
            "platform rows must be distinguishable from raw provider rows",
        );
    }
}
