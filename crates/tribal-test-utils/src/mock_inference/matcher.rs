//! Request matchers for conditional mock dispatch.
//!
//! [`CompletionMatcher`] and [`EmbeddingMatcher`] are newtype wrappers
//! around predicate closures. Named constructors cover common matching
//! patterns; `.and()` composes two matchers into one.

use tribal_domain::EmbeddingPurpose;
use tribal_inference::{CompletionRequest, EmbeddingRequest};

/// Predicate for matching [`CompletionRequest`] values.
///
/// Used with [`MockInferenceProviderBuilder::when`] to register
/// conditional responses that fire on every matching call.
pub struct CompletionMatcher {
    predicate: Box<dyn Fn(&CompletionRequest) -> bool + Send + Sync>,
}

impl CompletionMatcher {
    /// Creates a matcher from a raw predicate closure.
    pub fn new(f: impl Fn(&CompletionRequest) -> bool + Send + Sync + 'static) -> Self {
        Self {
            predicate: Box::new(f),
        }
    }

    /// Matches requests whose `model` field equals the given value.
    pub fn has_model(model: impl Into<String>) -> Self {
        let model = model.into();
        Self::new(move |req| req.model == model)
    }

    /// Matches requests whose system prompt contains the given text.
    pub fn system_contains(text: impl Into<String>) -> Self {
        let text = text.into();
        Self::new(move |req| {
            req.system
                .as_deref()
                .is_some_and(|s| s.contains(text.as_str()))
        })
    }

    /// Matches requests where any message content contains the given text.
    pub fn message_contains(text: impl Into<String>) -> Self {
        let text = text.into();
        Self::new(move |req| {
            req.messages
                .iter()
                .any(|m| m.content.contains(text.as_str()))
        })
    }

    /// Combines two matchers — both must return `true` for the
    /// composite to match.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::new(move |req| (self.predicate)(req) && (other.predicate)(req))
    }

    /// Tests whether the given request matches this predicate.
    pub(crate) fn matches(&self, request: &CompletionRequest) -> bool {
        (self.predicate)(request)
    }
}

/// Predicate for matching [`EmbeddingRequest`] values.
///
/// Used with [`MockEmbeddingProviderBuilder::when`] to register
/// conditional responses that fire on every matching call.
pub struct EmbeddingMatcher {
    predicate: Box<dyn Fn(&EmbeddingRequest) -> bool + Send + Sync>,
}

impl EmbeddingMatcher {
    /// Creates a matcher from a raw predicate closure.
    pub fn new(f: impl Fn(&EmbeddingRequest) -> bool + Send + Sync + 'static) -> Self {
        Self {
            predicate: Box::new(f),
        }
    }

    /// Matches requests whose `model` field equals the given value.
    pub fn has_model(model: impl Into<String>) -> Self {
        let model = model.into();
        Self::new(move |req| req.model == model)
    }

    /// Matches requests whose `input` field contains the given text.
    pub fn input_contains(text: impl Into<String>) -> Self {
        let text = text.into();
        Self::new(move |req| req.input.contains(text.as_str()))
    }

    /// Matches requests whose `purpose` field equals the given value.
    pub fn has_purpose(purpose: EmbeddingPurpose) -> Self {
        Self::new(move |req| req.purpose == purpose)
    }

    /// Combines two matchers — both must return `true` for the
    /// composite to match.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::new(move |req| (self.predicate)(req) && (other.predicate)(req))
    }

    /// Tests whether the given request matches this predicate.
    pub(crate) fn matches(&self, request: &EmbeddingRequest) -> bool {
        (self.predicate)(request)
    }
}

#[cfg(test)]
mod tests {
    use tribal_inference::{Message, Role};

    use super::*;

    fn a_completion_request(model: &str, system: Option<&str>) -> CompletionRequest {
        CompletionRequest {
            system: system.map(String::from),
            messages: vec![Message {
                role: Role::User,
                content: "test message".to_owned(),
            }],
            model: model.to_owned(),
            temperature: None,
            max_tokens: None,
            response_format: None,
        }
    }

    fn an_embedding_request(model: &str, input: &str) -> EmbeddingRequest {
        EmbeddingRequest {
            input: input.to_owned(),
            model: model.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        }
    }

    #[test]
    fn test_completion_has_model() {
        let matcher = CompletionMatcher::has_model("llama3");
        assert!(matcher.matches(&a_completion_request("llama3", None)));
        assert!(!matcher.matches(&a_completion_request("gpt-4", None)));
    }

    #[test]
    fn test_completion_system_contains() {
        let matcher = CompletionMatcher::system_contains("extraction");
        assert!(matcher.matches(&a_completion_request(
            "m",
            Some("You are an extraction agent")
        )));
        assert!(!matcher.matches(&a_completion_request("m", Some("triage only"))));
        assert!(!matcher.matches(&a_completion_request("m", None)));
    }

    #[test]
    fn test_completion_message_contains() {
        let mut req = a_completion_request("m", None);
        req.messages = vec![
            Message {
                role: Role::User,
                content: "first message".to_owned(),
            },
            Message {
                role: Role::Assistant,
                content: "keyword here".to_owned(),
            },
        ];
        let matcher = CompletionMatcher::message_contains("keyword");
        assert!(matcher.matches(&req));

        let matcher_miss = CompletionMatcher::message_contains("missing");
        assert!(!matcher_miss.matches(&req));
    }

    #[test]
    fn test_completion_and_composition() {
        let matcher = CompletionMatcher::has_model("llama3")
            .and(CompletionMatcher::system_contains("triage"));

        assert!(matcher.matches(&a_completion_request("llama3", Some("triage prompt"))));
        assert!(!matcher.matches(&a_completion_request("gpt-4", Some("triage prompt"))));
        assert!(!matcher.matches(&a_completion_request("llama3", Some("extraction prompt"))));
    }

    #[test]
    fn test_embedding_matchers() {
        let model_matcher = EmbeddingMatcher::has_model("nomic");
        assert!(model_matcher.matches(&an_embedding_request("nomic", "text")));
        assert!(!model_matcher.matches(&an_embedding_request("other", "text")));

        let input_matcher = EmbeddingMatcher::input_contains("knowledge");
        assert!(input_matcher.matches(&an_embedding_request("m", "some knowledge here")));
        assert!(!input_matcher.matches(&an_embedding_request("m", "nothing relevant")));

        let purpose_matcher = EmbeddingMatcher::has_purpose(EmbeddingPurpose::Candidate);
        assert!(purpose_matcher.matches(&an_embedding_request("m", "text")));

        let combined =
            EmbeddingMatcher::has_model("nomic").and(EmbeddingMatcher::input_contains("knowledge"));
        assert!(combined.matches(&an_embedding_request("nomic", "some knowledge")));
        assert!(!combined.matches(&an_embedding_request("nomic", "other text")));
        assert!(!combined.matches(&an_embedding_request("other", "some knowledge")));
    }
}
