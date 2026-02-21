//! Mock provider structs, fluent builders, and trait implementations.
//!
//! [`MockInferenceProvider`] implements [`InferenceProvider`] and
//! [`MockEmbeddingProvider`] implements [`EmbeddingProvider`]. Both are
//! constructed via fluent builders with sequential response queues,
//! conditional matching, and configurable exhaustion behaviour.

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use tribal_inference::{
    CompletionRequest, CompletionResponse, EmbeddingProvider, EmbeddingRequest, EmbeddingResponse,
    InferenceError, InferenceProvider,
};

use super::core::{
    ConditionalEntry, ConditionalOutcome, ExhaustBehaviour, MockProviderCore, QueueEntry,
};
use super::matcher::{CompletionMatcher, EmbeddingMatcher};
use super::responses::ErrorFactory;

const MUTEX_POISONED: &str = "mock provider mutex poisoned";

// ---------------------------------------------------------------------------
// Usage accumulators
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CompletionUsageAccumulator {
    total_input_tokens: u64,
    total_output_tokens: u64,
}

#[derive(Default)]
struct EmbeddingUsageAccumulator {
    total_tokens: u64,
}

// ---------------------------------------------------------------------------
// MockInferenceProvider
// ---------------------------------------------------------------------------

/// Mock implementation of [`InferenceProvider`] for deterministic testing.
///
/// Constructed via [`MockInferenceProvider::builder`]. Dispatches canned
/// responses from a sequential queue with optional conditional matching,
/// error injection, call history capture, and usage accounting.
pub struct MockInferenceProvider {
    core: MockProviderCore<CompletionRequest, CompletionResponse>,
    usage: Mutex<CompletionUsageAccumulator>,
}

impl MockInferenceProvider {
    /// Returns a new builder for configuring this mock.
    #[must_use]
    pub fn builder() -> MockInferenceProviderBuilder {
        MockInferenceProviderBuilder::new()
    }

    /// Returns a clone of all completion requests dispatched so far.
    pub fn completion_history(&self) -> Vec<CompletionRequest> {
        self.core.history()
    }

    /// Returns the number of calls dispatched so far.
    pub fn call_count(&self) -> usize {
        self.core.call_count()
    }

    /// Returns the cumulative input tokens across all successful calls.
    pub fn total_input_tokens(&self) -> u64 {
        self.usage.lock().expect(MUTEX_POISONED).total_input_tokens
    }

    /// Returns the cumulative output tokens across all successful calls.
    pub fn total_output_tokens(&self) -> u64 {
        self.usage.lock().expect(MUTEX_POISONED).total_output_tokens
    }

    /// Panics if the sequential queue has not been fully consumed.
    pub fn assert_exhausted(&self) {
        self.core.assert_exhausted();
    }
}

#[async_trait]
impl InferenceProvider for MockInferenceProvider {
    async fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse, InferenceError> {
        let result = self.core.dispatch(request)?;
        let mut usage = self.usage.lock().expect(MUTEX_POISONED);
        usage.total_input_tokens += u64::from(result.usage.input_tokens);
        usage.total_output_tokens += u64::from(result.usage.output_tokens);
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// MockInferenceProviderBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for [`MockInferenceProvider`].
#[must_use]
pub struct MockInferenceProviderBuilder {
    queue: VecDeque<QueueEntry<CompletionResponse>>,
    conditionals: Vec<ConditionalEntry<CompletionRequest, CompletionResponse>>,
    exhaust_behaviour: ExhaustBehaviour,
}

impl MockInferenceProviderBuilder {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            conditionals: Vec::new(),
            exhaust_behaviour: ExhaustBehaviour::Panic,
        }
    }

    /// Enqueues a successful completion response (FIFO).
    #[must_use]
    pub fn on_complete(mut self, response: CompletionResponse) -> Self {
        self.queue.push_back(QueueEntry::Ok(response));
        self
    }

    /// Enqueues an error factory that will fire on the corresponding call.
    #[must_use]
    pub fn on_complete_error(
        mut self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
    ) -> Self {
        self.queue.push_back(QueueEntry::Err(Box::new(factory)));
        self
    }

    /// Begins a conditional entry — returns a scoped builder that
    /// captures the matcher and provides `respond_with` /
    /// `respond_with_error` to complete the entry.
    #[must_use]
    pub fn when(self, matcher: CompletionMatcher) -> ConditionalCompletionBuilder {
        ConditionalCompletionBuilder {
            parent: self,
            matcher,
        }
    }

    /// Sets the behaviour when the sequential queue is exhausted.
    #[must_use]
    pub fn on_exhaust(mut self, behaviour: ExhaustBehaviour) -> Self {
        self.exhaust_behaviour = behaviour;
        self
    }

    /// Builds the [`MockInferenceProvider`].
    pub fn build(self) -> MockInferenceProvider {
        MockInferenceProvider {
            core: MockProviderCore::new(
                "MockInferenceProvider",
                self.queue,
                self.conditionals,
                self.exhaust_behaviour,
            ),
            usage: Mutex::new(CompletionUsageAccumulator::default()),
        }
    }
}

// ---------------------------------------------------------------------------
// ConditionalCompletionBuilder
// ---------------------------------------------------------------------------

/// Scoped builder for a conditional completion entry.
///
/// Returned by [`MockInferenceProviderBuilder::when`]. Completes with
/// `respond_with` or `respond_with_error`, returning control to the
/// parent builder.
#[must_use]
pub struct ConditionalCompletionBuilder {
    parent: MockInferenceProviderBuilder,
    matcher: CompletionMatcher,
}

impl ConditionalCompletionBuilder {
    /// Registers a successful response for this conditional.
    #[must_use]
    pub fn respond_with(self, response: CompletionResponse) -> MockInferenceProviderBuilder {
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Ok(response),
        });
        parent
    }

    /// Registers an error factory for this conditional.
    #[must_use]
    pub fn respond_with_error(
        self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
    ) -> MockInferenceProviderBuilder {
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Err(Box::new(factory)),
        });
        parent
    }
}

// ---------------------------------------------------------------------------
// MockEmbeddingProvider
// ---------------------------------------------------------------------------

/// Mock implementation of [`EmbeddingProvider`] for deterministic testing.
///
/// Constructed via [`MockEmbeddingProvider::builder`]. Dispatches canned
/// responses from a sequential queue with optional conditional matching,
/// error injection, call history capture, and usage accounting.
pub struct MockEmbeddingProvider {
    core: MockProviderCore<EmbeddingRequest, EmbeddingResponse>,
    usage: Mutex<EmbeddingUsageAccumulator>,
}

impl MockEmbeddingProvider {
    /// Returns a new builder for configuring this mock.
    #[must_use]
    pub fn builder() -> MockEmbeddingProviderBuilder {
        MockEmbeddingProviderBuilder::new()
    }

    /// Returns a clone of all embedding requests dispatched so far.
    pub fn embedding_history(&self) -> Vec<EmbeddingRequest> {
        self.core.history()
    }

    /// Returns the number of calls dispatched so far.
    pub fn call_count(&self) -> usize {
        self.core.call_count()
    }

    /// Returns the cumulative total tokens across all successful calls.
    pub fn total_tokens(&self) -> u64 {
        self.usage.lock().expect(MUTEX_POISONED).total_tokens
    }

    /// Panics if the sequential queue has not been fully consumed.
    pub fn assert_exhausted(&self) {
        self.core.assert_exhausted();
    }
}

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> Result<EmbeddingResponse, InferenceError> {
        let result = self.core.dispatch(request)?;
        let mut usage = self.usage.lock().expect(MUTEX_POISONED);
        usage.total_tokens += u64::from(result.usage.total_tokens);
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// MockEmbeddingProviderBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for [`MockEmbeddingProvider`].
#[must_use]
pub struct MockEmbeddingProviderBuilder {
    queue: VecDeque<QueueEntry<EmbeddingResponse>>,
    conditionals: Vec<ConditionalEntry<EmbeddingRequest, EmbeddingResponse>>,
    exhaust_behaviour: ExhaustBehaviour,
}

impl MockEmbeddingProviderBuilder {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            conditionals: Vec::new(),
            exhaust_behaviour: ExhaustBehaviour::Panic,
        }
    }

    /// Enqueues a successful embedding response (FIFO).
    #[must_use]
    pub fn on_embed(mut self, response: EmbeddingResponse) -> Self {
        self.queue.push_back(QueueEntry::Ok(response));
        self
    }

    /// Enqueues an error factory that will fire on the corresponding call.
    #[must_use]
    pub fn on_embed_error(
        mut self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
    ) -> Self {
        self.queue.push_back(QueueEntry::Err(Box::new(factory)));
        self
    }

    /// Begins a conditional entry — returns a scoped builder that
    /// captures the matcher and provides `respond_with` /
    /// `respond_with_error` to complete the entry.
    #[must_use]
    pub fn when(self, matcher: EmbeddingMatcher) -> ConditionalEmbeddingBuilder {
        ConditionalEmbeddingBuilder {
            parent: self,
            matcher,
        }
    }

    /// Sets the behaviour when the sequential queue is exhausted.
    #[must_use]
    pub fn on_exhaust(mut self, behaviour: ExhaustBehaviour) -> Self {
        self.exhaust_behaviour = behaviour;
        self
    }

    /// Builds the [`MockEmbeddingProvider`].
    pub fn build(self) -> MockEmbeddingProvider {
        MockEmbeddingProvider {
            core: MockProviderCore::new(
                "MockEmbeddingProvider",
                self.queue,
                self.conditionals,
                self.exhaust_behaviour,
            ),
            usage: Mutex::new(EmbeddingUsageAccumulator::default()),
        }
    }
}

// ---------------------------------------------------------------------------
// ConditionalEmbeddingBuilder
// ---------------------------------------------------------------------------

/// Scoped builder for a conditional embedding entry.
///
/// Returned by [`MockEmbeddingProviderBuilder::when`]. Completes with
/// `respond_with` or `respond_with_error`, returning control to the
/// parent builder.
#[must_use]
pub struct ConditionalEmbeddingBuilder {
    parent: MockEmbeddingProviderBuilder,
    matcher: EmbeddingMatcher,
}

impl ConditionalEmbeddingBuilder {
    /// Registers a successful response for this conditional.
    #[must_use]
    pub fn respond_with(self, response: EmbeddingResponse) -> MockEmbeddingProviderBuilder {
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Ok(response),
        });
        parent
    }

    /// Registers an error factory for this conditional.
    #[must_use]
    pub fn respond_with_error(
        self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
    ) -> MockEmbeddingProviderBuilder {
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Err(Box::new(factory)),
        });
        parent
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_domain::EmbeddingPurpose;
    use tribal_inference::{Message, Role};

    use super::*;
    use crate::mock_inference::responses::{
        a_completion_response, a_provider_unavailable, an_embedding_response,
    };

    fn a_request(model: &str, system: &str) -> CompletionRequest {
        CompletionRequest {
            system: Some(system.to_owned()),
            messages: vec![Message {
                role: Role::User,
                content: "test".to_owned(),
            }],
            model: model.to_owned(),
            temperature: None,
            max_tokens: None,
            response_format: None,
        }
    }

    fn an_embed_request(model: &str, input: &str) -> EmbeddingRequest {
        EmbeddingRequest {
            input: input.to_owned(),
            model: model.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        }
    }

    // -- Inference provider tests ------------------------------------------

    #[tokio::test]
    async fn test_sequential_responses_returned_in_fifo_order() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("first"))
            .on_complete(a_completion_response("second"))
            .on_complete(a_completion_response("third"))
            .build();

        let r1 = provider.complete(a_request("m", "sys")).await.unwrap();
        let r2 = provider.complete(a_request("m", "sys")).await.unwrap();
        let r3 = provider.complete(a_request("m", "sys")).await.unwrap();

        assert_eq!(r1.text, "first");
        assert_eq!(r2.text, "second");
        assert_eq!(r3.text, "third");
    }

    #[tokio::test]
    async fn test_conditional_match_takes_priority_over_sequential() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("sequential"))
            .when(CompletionMatcher::has_model("special"))
                .respond_with(a_completion_response("conditional"))
            .build();

        let cond = provider.complete(a_request("special", "sys")).await.unwrap();
        assert_eq!(cond.text, "conditional");

        // Sequential entry is still available.
        let seq = provider.complete(a_request("other", "sys")).await.unwrap();
        assert_eq!(seq.text, "sequential");
    }

    #[tokio::test]
    async fn test_conditional_entries_fire_repeatedly() {
        let provider = MockInferenceProvider::builder()
            .when(CompletionMatcher::has_model("repeat"))
                .respond_with(a_completion_response("again"))
            .build();

        for _ in 0..5 {
            let resp = provider.complete(a_request("repeat", "sys")).await.unwrap();
            assert_eq!(resp.text, "again");
        }
        assert_eq!(provider.call_count(), 5);
    }

    #[tokio::test]
    async fn test_conditional_error_response() {
        let provider = MockInferenceProvider::builder()
            .when(CompletionMatcher::has_model("fail"))
                .respond_with_error(a_provider_unavailable("conditional failure"))
            .build();

        let err = provider
            .complete(a_request("fail", "sys"))
            .await
            .unwrap_err();
        assert!(matches!(err, InferenceError::ProviderUnavailable { .. }));
    }

    #[tokio::test]
    #[should_panic(expected = "sequential queue exhausted")]
    async fn test_conditional_only_no_match_triggers_exhaustion() {
        let provider = MockInferenceProvider::builder()
            .when(CompletionMatcher::has_model("specific"))
                .respond_with(a_completion_response("match"))
            .build();

        // This model doesn't match any conditional, and the queue is empty.
        let _ = provider.complete(a_request("other", "sys")).await;
    }

    #[tokio::test]
    #[should_panic(expected = "sequential queue exhausted")]
    async fn test_exhaust_panic_on_empty_queue() {
        let provider = MockInferenceProvider::builder().build();
        let _ = provider.complete(a_request("m", "sys")).await;
    }

    #[tokio::test]
    async fn test_exhaust_repeat_last_clones_last_success() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("only"))
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build();

        let r1 = provider.complete(a_request("m", "sys")).await.unwrap();
        let r2 = provider.complete(a_request("m", "sys")).await.unwrap();
        let r3 = provider.complete(a_request("m", "sys")).await.unwrap();

        assert_eq!(r1.text, "only");
        assert_eq!(r2.text, "only");
        assert_eq!(r3.text, "only");
    }

    #[tokio::test]
    #[should_panic(expected = "no prior successful sequential response exists")]
    async fn test_exhaust_repeat_last_no_prior_success_panics() {
        let provider = MockInferenceProvider::builder()
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build();

        let _ = provider.complete(a_request("m", "sys")).await;
    }

    #[tokio::test]
    async fn test_exhaust_error_returns_factory_output() {
        let provider = MockInferenceProvider::builder()
            .on_exhaust(ExhaustBehaviour::Error(
                a_provider_unavailable("rate limited"),
            ))
            .build();

        let err = provider
            .complete(a_request("m", "sys"))
            .await
            .unwrap_err();
        assert!(matches!(err, InferenceError::ProviderUnavailable { .. }));

        // Fires repeatedly.
        let err2 = provider
            .complete(a_request("m", "sys"))
            .await
            .unwrap_err();
        assert!(matches!(err2, InferenceError::ProviderUnavailable { .. }));
    }

    #[tokio::test]
    async fn test_error_injection_via_on_complete_error() {
        let provider = MockInferenceProvider::builder()
            .on_complete_error(a_provider_unavailable("first fails"))
            .on_complete(a_completion_response("retry succeeds"))
            .build();

        let err = provider
            .complete(a_request("m", "sys"))
            .await
            .unwrap_err();
        assert!(matches!(err, InferenceError::ProviderUnavailable { .. }));

        let ok = provider.complete(a_request("m", "sys")).await.unwrap();
        assert_eq!(ok.text, "retry succeeds");
    }

    #[tokio::test]
    async fn test_call_history_records_all_requests() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("ok"))
            .on_complete_error(a_provider_unavailable("fail"))
            .build();

        let _ = provider.complete(a_request("model-a", "sys-a")).await;
        let _ = provider.complete(a_request("model-b", "sys-b")).await;

        let history = provider.completion_history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].model, "model-a");
        assert_eq!(history[1].model, "model-b");
    }

    #[tokio::test]
    async fn test_usage_accounting_only_counts_successes() {
        let provider = MockInferenceProvider::builder()
            .on_complete_error(a_provider_unavailable("fail"))
            .on_complete(a_completion_response("ok"))
            .on_complete(a_completion_response("ok2"))
            .build();

        let _ = provider.complete(a_request("m", "sys")).await; // error
        let _ = provider.complete(a_request("m", "sys")).await; // success
        let _ = provider.complete(a_request("m", "sys")).await; // success

        assert_eq!(provider.total_input_tokens(), 200);
        assert_eq!(provider.total_output_tokens(), 100);
    }

    #[tokio::test]
    #[should_panic(expected = "sequential responses consumed")]
    async fn test_assert_exhausted_panics_with_remaining() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("a"))
            .on_complete(a_completion_response("b"))
            .build();

        let _ = provider.complete(a_request("m", "sys")).await;
        // Only consumed 1 of 2.
        provider.assert_exhausted();
    }

    #[tokio::test]
    async fn test_assert_exhausted_passes_when_fully_consumed() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("only"))
            .build();

        let _ = provider.complete(a_request("m", "sys")).await;
        provider.assert_exhausted(); // should not panic
    }

    #[test]
    fn test_send_sync_inference_provider() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockInferenceProvider>();

        // Verify trait-object construction compiles.
        let provider = MockInferenceProvider::builder().build();
        let _arc: Arc<dyn InferenceProvider> = Arc::new(provider);
    }

    // -- Embedding provider tests ------------------------------------------

    #[test]
    fn test_send_sync_embedding_provider() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockEmbeddingProvider>();

        let provider = MockEmbeddingProvider::builder().build();
        let _arc: Arc<dyn EmbeddingProvider> = Arc::new(provider);
    }

    #[tokio::test]
    async fn test_embedding_sequential_delivery() {
        let provider = MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1, 0.2]))
            .on_embed(an_embedding_response(vec![0.3, 0.4]))
            .build();

        let r1 = provider
            .embed(an_embed_request("m", "first"))
            .await
            .unwrap();
        let r2 = provider
            .embed(an_embed_request("m", "second"))
            .await
            .unwrap();

        assert_eq!(r1.vector, vec![0.1, 0.2]);
        assert_eq!(r2.vector, vec![0.3, 0.4]);
        provider.assert_exhausted();
    }

    #[tokio::test]
    async fn test_embedding_usage_accounting() {
        let provider = MockEmbeddingProvider::builder()
            .on_embed_error(a_provider_unavailable("fail"))
            .on_embed(an_embedding_response(vec![0.1]))
            .on_embed(an_embedding_response(vec![0.2]))
            .build();

        let _ = provider.embed(an_embed_request("m", "a")).await; // error
        let _ = provider.embed(an_embed_request("m", "b")).await; // success
        let _ = provider.embed(an_embed_request("m", "c")).await; // success

        // Default mock embedding has total_tokens = 10.
        assert_eq!(provider.total_tokens(), 20);
    }

    #[tokio::test]
    async fn test_embedding_conditional_with_has_purpose() {
        let provider = MockEmbeddingProvider::builder()
            .when(EmbeddingMatcher::has_purpose(EmbeddingPurpose::Query))
                .respond_with(an_embedding_response(vec![1.0, 0.0]))
            .on_embed(an_embedding_response(vec![0.0, 1.0]))
            .build();

        // Query purpose matches the conditional.
        let query_req = EmbeddingRequest {
            input: "search text".to_owned(),
            model: "m".to_owned(),
            purpose: EmbeddingPurpose::Query,
        };
        let resp = provider.embed(query_req).await.unwrap();
        assert_eq!(resp.vector, vec![1.0, 0.0]);

        // Candidate purpose falls through to sequential.
        let candidate_req = an_embed_request("m", "index text");
        let resp = provider.embed(candidate_req).await.unwrap();
        assert_eq!(resp.vector, vec![0.0, 1.0]);

        provider.assert_exhausted();
    }
}
