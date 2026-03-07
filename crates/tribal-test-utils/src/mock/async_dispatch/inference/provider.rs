//! Mock provider structs, fluent builders, and trait implementations.
//!
//! [`MockInferenceProvider`] implements [`InferenceProvider`] and
//! [`MockEmbeddingProvider`] implements [`EmbeddingProvider`]. Both are
//! constructed via fluent builders with sequential response queues,
//! conditional matching, and configurable exhaustion behaviour.

use std::{collections::VecDeque, sync::Mutex};

use async_trait::async_trait;
use tribal_inference::{
    CompletionRequest, CompletionResponse, EmbeddingProvider, EmbeddingRequest, EmbeddingResponse,
    InferenceError, InferenceProvider, ProviderIdentity,
};

use crate::mock::async_dispatch::core::{
    ConditionalEntry, ConditionalOutcome, ExhaustBehaviour, MUTEX_POISONED,
    MockAsyncDispatchCore, MockProviderOptions, QueueEntry,
};

use super::matcher::{CompletionMatcher, EmbeddingMatcher};

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
    core: MockAsyncDispatchCore<CompletionRequest, CompletionResponse, InferenceError>,
    usage: Mutex<CompletionUsageAccumulator>,
    identity: ProviderIdentity,
}

impl MockInferenceProvider {
    /// Returns a new builder for configuring this mock.
    pub fn builder() -> MockInferenceProviderBuilder {
        MockInferenceProviderBuilder::new()
    }

    /// Returns a clone of all completion requests dispatched so far.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn completion_history(&self) -> Vec<CompletionRequest> {
        self.core.history()
    }

    /// Returns the number of calls dispatched so far.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn call_count(&self) -> usize {
        self.core.call_count()
    }

    /// Returns the cumulative input tokens across all successful calls.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn total_input_tokens(&self) -> u64 {
        self.usage.lock().expect(MUTEX_POISONED).total_input_tokens
    }

    /// Returns the cumulative output tokens across all successful calls.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn total_output_tokens(&self) -> u64 {
        self.usage.lock().expect(MUTEX_POISONED).total_output_tokens
    }

    /// Panics if the sequential queue has not been fully consumed.
    ///
    /// # Panics
    ///
    /// Panics if sequential entries remain unconsumed, or if the
    /// internal mutex is poisoned.
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
        let (result, delay) = self.core.dispatch(&request);
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let result = result?;
        let mut usage = self.usage.lock().expect(MUTEX_POISONED);
        usage.total_input_tokens += u64::from(result.usage.input_tokens);
        usage.total_output_tokens += u64::from(result.usage.output_tokens);
        Ok(result)
    }

    fn identity(&self) -> &ProviderIdentity {
        &self.identity
    }
}

// ---------------------------------------------------------------------------
// MockInferenceProviderBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for [`MockInferenceProvider`].
#[must_use]
pub struct MockInferenceProviderBuilder {
    queue: VecDeque<QueueEntry<CompletionResponse, InferenceError>>,
    conditionals: Vec<ConditionalEntry<CompletionRequest, CompletionResponse, InferenceError>>,
    exhaust_behaviour: ExhaustBehaviour<InferenceError>,
    identity: ProviderIdentity,
}

impl MockInferenceProviderBuilder {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            conditionals: Vec::new(),
            exhaust_behaviour: ExhaustBehaviour::Panic,
            identity: ProviderIdentity {
                name: "mock".into(),
                model: "mock-model".into(),
            },
        }
    }

    /// Sets the provider identity returned by [`InferenceProvider::identity`].
    pub fn with_identity(mut self, identity: ProviderIdentity) -> Self {
        self.identity = identity;
        self
    }

    /// Enqueues a successful completion response (FIFO).
    pub fn on_complete(
        mut self,
        response: CompletionResponse,
        options: Option<MockProviderOptions>,
    ) -> Self {
        let delay = options.and_then(|o| o.delay);
        self.queue.push_back(QueueEntry::Ok(response, delay));
        self
    }

    /// Enqueues an error factory that will fire on the corresponding call.
    pub fn on_complete_error(
        mut self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
        options: Option<MockProviderOptions>,
    ) -> Self {
        let delay = options.and_then(|o| o.delay);
        self.queue
            .push_back(QueueEntry::Err(Box::new(factory), delay));
        self
    }

    /// Begins a conditional entry — returns a scoped builder that
    /// captures the matcher and provides `respond_with` /
    /// `respond_with_error` to complete the entry.
    pub fn when(self, matcher: CompletionMatcher) -> ConditionalCompletionBuilder {
        ConditionalCompletionBuilder {
            parent: self,
            matcher,
        }
    }

    /// Sets the behaviour when the sequential queue is exhausted.
    pub fn on_exhaust(mut self, behaviour: ExhaustBehaviour<InferenceError>) -> Self {
        self.exhaust_behaviour = behaviour;
        self
    }

    /// Builds the [`MockInferenceProvider`].
    pub fn build(self) -> MockInferenceProvider {
        MockInferenceProvider {
            core: MockAsyncDispatchCore::new(
                "MockInferenceProvider",
                self.queue,
                self.conditionals,
                self.exhaust_behaviour,
            ),
            usage: Mutex::new(CompletionUsageAccumulator::default()),
            identity: self.identity,
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
    pub fn respond_with(
        self,
        response: CompletionResponse,
        options: Option<MockProviderOptions>,
    ) -> MockInferenceProviderBuilder {
        let delay = options.and_then(|o| o.delay);
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Ok(response, delay),
        });
        parent
    }

    /// Registers an error factory for this conditional.
    pub fn respond_with_error(
        self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
        options: Option<MockProviderOptions>,
    ) -> MockInferenceProviderBuilder {
        let delay = options.and_then(|o| o.delay);
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Err(Box::new(factory), delay),
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
    core: MockAsyncDispatchCore<EmbeddingRequest, EmbeddingResponse, InferenceError>,
    usage: Mutex<EmbeddingUsageAccumulator>,
    identity: ProviderIdentity,
}

impl MockEmbeddingProvider {
    /// Returns a new builder for configuring this mock.
    pub fn builder() -> MockEmbeddingProviderBuilder {
        MockEmbeddingProviderBuilder::new()
    }

    /// Returns a clone of all embedding requests dispatched so far.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn embedding_history(&self) -> Vec<EmbeddingRequest> {
        self.core.history()
    }

    /// Returns the number of calls dispatched so far.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn call_count(&self) -> usize {
        self.core.call_count()
    }

    /// Returns the cumulative total tokens across all successful calls.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn total_tokens(&self) -> u64 {
        self.usage.lock().expect(MUTEX_POISONED).total_tokens
    }

    /// Panics if the sequential queue has not been fully consumed.
    ///
    /// # Panics
    ///
    /// Panics if sequential entries remain unconsumed, or if the
    /// internal mutex is poisoned.
    pub fn assert_exhausted(&self) {
        self.core.assert_exhausted();
    }
}

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, InferenceError> {
        let (result, delay) = self.core.dispatch(&request);
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let result = result?;
        let mut usage = self.usage.lock().expect(MUTEX_POISONED);
        usage.total_tokens += u64::from(result.usage.total_tokens);
        Ok(result)
    }

    fn identity(&self) -> &ProviderIdentity {
        &self.identity
    }
}

// ---------------------------------------------------------------------------
// MockEmbeddingProviderBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for [`MockEmbeddingProvider`].
#[must_use]
pub struct MockEmbeddingProviderBuilder {
    queue: VecDeque<QueueEntry<EmbeddingResponse, InferenceError>>,
    conditionals: Vec<ConditionalEntry<EmbeddingRequest, EmbeddingResponse, InferenceError>>,
    exhaust_behaviour: ExhaustBehaviour<InferenceError>,
    identity: ProviderIdentity,
}

impl MockEmbeddingProviderBuilder {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            conditionals: Vec::new(),
            exhaust_behaviour: ExhaustBehaviour::Panic,
            identity: ProviderIdentity {
                name: "mock".into(),
                model: "mock-model".into(),
            },
        }
    }

    /// Sets the provider identity returned by [`EmbeddingProvider::identity`].
    pub fn with_identity(mut self, identity: ProviderIdentity) -> Self {
        self.identity = identity;
        self
    }

    /// Enqueues a successful embedding response (FIFO).
    pub fn on_embed(
        mut self,
        response: EmbeddingResponse,
        options: Option<MockProviderOptions>,
    ) -> Self {
        let delay = options.and_then(|o| o.delay);
        self.queue.push_back(QueueEntry::Ok(response, delay));
        self
    }

    /// Enqueues an error factory that will fire on the corresponding call.
    pub fn on_embed_error(
        mut self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
        options: Option<MockProviderOptions>,
    ) -> Self {
        let delay = options.and_then(|o| o.delay);
        self.queue
            .push_back(QueueEntry::Err(Box::new(factory), delay));
        self
    }

    /// Begins a conditional entry — returns a scoped builder that
    /// captures the matcher and provides `respond_with` /
    /// `respond_with_error` to complete the entry.
    pub fn when(self, matcher: EmbeddingMatcher) -> ConditionalEmbeddingBuilder {
        ConditionalEmbeddingBuilder {
            parent: self,
            matcher,
        }
    }

    /// Sets the behaviour when the sequential queue is exhausted.
    pub fn on_exhaust(mut self, behaviour: ExhaustBehaviour<InferenceError>) -> Self {
        self.exhaust_behaviour = behaviour;
        self
    }

    /// Builds the [`MockEmbeddingProvider`].
    pub fn build(self) -> MockEmbeddingProvider {
        MockEmbeddingProvider {
            core: MockAsyncDispatchCore::new(
                "MockEmbeddingProvider",
                self.queue,
                self.conditionals,
                self.exhaust_behaviour,
            ),
            usage: Mutex::new(EmbeddingUsageAccumulator::default()),
            identity: self.identity,
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
    pub fn respond_with(
        self,
        response: EmbeddingResponse,
        options: Option<MockProviderOptions>,
    ) -> MockEmbeddingProviderBuilder {
        let delay = options.and_then(|o| o.delay);
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Ok(response, delay),
        });
        parent
    }

    /// Registers an error factory for this conditional.
    pub fn respond_with_error(
        self,
        factory: impl Fn() -> InferenceError + Send + Sync + 'static,
        options: Option<MockProviderOptions>,
    ) -> MockEmbeddingProviderBuilder {
        let delay = options.and_then(|o| o.delay);
        let mut parent = self.parent;
        parent.conditionals.push(ConditionalEntry {
            matcher: Box::new(move |req| self.matcher.matches(req)),
            outcome: ConditionalOutcome::Err(Box::new(factory), delay),
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
    use crate::mock::async_dispatch::inference::responses::{
        a_completion_response, a_provider_unavailable, an_embedding_response,
    };

    fn a_request(system: &str) -> CompletionRequest {
        CompletionRequest {
            system: Some(system.to_owned()),
            messages: vec![Message {
                role: Role::User,
                content: "test".to_owned(),
            }],
            temperature: None,
            max_tokens: None,
            response_format: None,
        }
    }

    fn an_embed_request(input: &str) -> EmbeddingRequest {
        EmbeddingRequest {
            input: input.to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        }
    }

    // -- Inference provider tests ------------------------------------------

    #[tokio::test]
    async fn test_sequential_responses_returned_in_fifo_order() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("first"), None)
            .on_complete(a_completion_response("second"), None)
            .on_complete(a_completion_response("third"), None)
            .build();

        let r1 = provider.complete(a_request("sys")).await.unwrap();
        let r2 = provider.complete(a_request("sys")).await.unwrap();
        let r3 = provider.complete(a_request("sys")).await.unwrap();

        assert_eq!(r1.text, "first");
        assert_eq!(r2.text, "second");
        assert_eq!(r3.text, "third");
    }

    #[tokio::test]
    async fn test_conditional_match_takes_priority_over_sequential() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("sequential"), None)
            .when(CompletionMatcher::system_contains("special"))
            .respond_with(a_completion_response("conditional"), None)
            .build();

        // Conditional matches first despite sequential entry existing.
        let cond = provider.complete(a_request("special")).await.unwrap();
        assert_eq!(cond.text, "conditional");

        // Sequential entry was not consumed by the conditional call.
        let seq = provider.complete(a_request("other")).await.unwrap();
        assert_eq!(seq.text, "sequential");
    }

    #[tokio::test]
    async fn test_conditional_entries_fire_repeatedly() {
        let provider = MockInferenceProvider::builder()
            .when(CompletionMatcher::system_contains("repeat"))
            .respond_with(a_completion_response("again"), None)
            .build();

        for i in 0..5 {
            let resp = provider.complete(a_request("repeat")).await.unwrap();
            assert_eq!(
                resp.text, "again",
                "call {i} should return conditional response"
            );
        }
        assert_eq!(provider.call_count(), 5);
    }

    #[tokio::test]
    async fn test_conditional_error_response() {
        let provider = MockInferenceProvider::builder()
            .when(CompletionMatcher::system_contains("fail"))
            .respond_with_error(a_provider_unavailable("conditional failure"), None)
            .build();

        let err = provider.complete(a_request("fail")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason == "conditional failure"
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "sequential queue exhausted")]
    async fn test_conditional_only_no_match_triggers_exhaustion() {
        let provider = MockInferenceProvider::builder()
            .when(CompletionMatcher::system_contains("specific"))
            .respond_with(a_completion_response("match"), None)
            .build();

        let _ = provider.complete(a_request("other")).await;
    }

    #[tokio::test]
    #[should_panic(expected = "sequential queue exhausted")]
    async fn test_exhaust_panic_on_empty_queue() {
        let provider = MockInferenceProvider::builder().build();
        let _ = provider.complete(a_request("sys")).await;
    }

    #[tokio::test]
    async fn test_exhaust_repeat_last_clones_last_success() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("only"), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build();

        // First call consumes the sequential entry.
        let r1 = provider.complete(a_request("sys")).await.unwrap();
        assert_eq!(r1.text, "only");

        // Subsequent calls clone the last successful response.
        let r2 = provider.complete(a_request("sys")).await.unwrap();
        let r3 = provider.complete(a_request("sys")).await.unwrap();
        assert_eq!(r2.text, "only");
        assert_eq!(r3.text, "only");
        assert_eq!(provider.call_count(), 3);
    }

    #[tokio::test]
    #[should_panic(expected = "no prior successful sequential response exists")]
    async fn test_exhaust_repeat_last_no_prior_success_panics() {
        let provider = MockInferenceProvider::builder()
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build();

        let _ = provider.complete(a_request("sys")).await;
    }

    #[tokio::test]
    async fn test_exhaust_error_returns_factory_output() {
        let provider = MockInferenceProvider::builder()
            .on_exhaust(ExhaustBehaviour::Error(a_provider_unavailable(
                "rate limited",
            )))
            .build();

        let err = provider.complete(a_request("sys")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason == "rate limited"
        ));

        // Fires repeatedly with fresh error instances.
        let err2 = provider.complete(a_request("sys")).await.unwrap_err();
        assert!(matches!(
            err2,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason == "rate limited"
        ));
    }

    #[tokio::test]
    async fn test_error_injection_via_on_complete_error() {
        let provider = MockInferenceProvider::builder()
            .on_complete_error(a_provider_unavailable("first fails"), None)
            .on_complete(a_completion_response("retry succeeds"), None)
            .build();

        let err = provider.complete(a_request("sys")).await.unwrap_err();
        assert!(matches!(
            err,
            InferenceError::ProviderUnavailable { ref reason, .. }
            if reason == "first fails"
        ));

        let ok = provider.complete(a_request("sys")).await.unwrap();
        assert_eq!(ok.text, "retry succeeds");
    }

    #[tokio::test]
    async fn test_call_history_records_all_requests() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("ok"), None)
            .on_complete_error(a_provider_unavailable("fail"), None)
            .build();

        let _ = provider.complete(a_request("sys-a")).await;
        let _ = provider.complete(a_request("sys-b")).await;

        let history = provider.completion_history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].system.as_deref(), Some("sys-a"));
        assert_eq!(history[1].system.as_deref(), Some("sys-b"));
    }

    #[tokio::test]
    async fn test_usage_accounting_only_counts_successes() {
        let provider = MockInferenceProvider::builder()
            .on_complete_error(a_provider_unavailable("fail"), None)
            .on_complete(a_completion_response("ok"), None)
            .on_complete(a_completion_response("ok2"), None)
            .build();

        let _ = provider.complete(a_request("sys")).await; // error — no usage
        let _ = provider.complete(a_request("sys")).await; // 100 input, 50 output
        let _ = provider.complete(a_request("sys")).await; // 100 input, 50 output

        // Two successful calls × 100 input tokens each.
        assert_eq!(provider.total_input_tokens(), 200);
        // Two successful calls × 50 output tokens each.
        assert_eq!(provider.total_output_tokens(), 100);
    }

    #[tokio::test]
    #[should_panic(expected = "sequential responses consumed")]
    async fn test_assert_exhausted_panics_with_remaining() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("a"), None)
            .on_complete(a_completion_response("b"), None)
            .build();

        let _ = provider.complete(a_request("sys")).await;
        provider.assert_exhausted();
    }

    #[tokio::test]
    async fn test_assert_exhausted_passes_when_fully_consumed() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("only"), None)
            .build();

        let _ = provider.complete(a_request("sys")).await;
        provider.assert_exhausted();
    }

    #[test]
    fn test_send_sync_inference_provider() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockInferenceProvider>();

        let provider = MockInferenceProvider::builder().build();
        let _arc: Arc<dyn InferenceProvider> = Arc::new(provider);
    }

    // -- Embedding provider tests ------------------------------------------

    #[tokio::test]
    async fn test_embedding_sequential_delivery() {
        let provider = MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.1, 0.2]), None)
            .on_embed(an_embedding_response(vec![0.3, 0.4]), None)
            .build();

        let r1 = provider.embed(an_embed_request("first")).await.unwrap();
        let r2 = provider.embed(an_embed_request("second")).await.unwrap();

        assert_eq!(r1.vector, vec![0.1, 0.2]);
        assert_eq!(r2.vector, vec![0.3, 0.4]);
        assert_eq!(provider.call_count(), 2);
        provider.assert_exhausted();
    }

    #[tokio::test]
    async fn test_embedding_usage_accounting() {
        let provider = MockEmbeddingProvider::builder()
            .on_embed_error(a_provider_unavailable("fail"), None)
            .on_embed(an_embedding_response(vec![0.1]), None)
            .on_embed(an_embedding_response(vec![0.2]), None)
            .build();

        let _ = provider.embed(an_embed_request("a")).await; // error — no usage
        let _ = provider.embed(an_embed_request("b")).await; // 10 tokens
        let _ = provider.embed(an_embed_request("c")).await; // 10 tokens

        // Two successful calls × 10 total_tokens each.
        assert_eq!(provider.total_tokens(), 20);
        assert_eq!(provider.call_count(), 3);
    }

    #[tokio::test]
    async fn test_embedding_conditional_with_has_purpose() {
        let provider = MockEmbeddingProvider::builder()
            .when(EmbeddingMatcher::has_purpose(EmbeddingPurpose::Query))
            .respond_with(an_embedding_response(vec![1.0, 0.0]), None)
            .on_embed(an_embedding_response(vec![0.0, 1.0]), None)
            .build();

        // Query purpose matches the conditional.
        let query_req = EmbeddingRequest {
            input: "search text".to_owned(),
            purpose: EmbeddingPurpose::Query,
        };
        let resp = provider.embed(query_req).await.unwrap();
        assert_eq!(resp.vector, vec![1.0, 0.0]);

        // Candidate purpose falls through to sequential.
        let candidate_req = an_embed_request("index text");
        let resp = provider.embed(candidate_req).await.unwrap();
        assert_eq!(resp.vector, vec![0.0, 1.0]);

        provider.assert_exhausted();
    }

    #[test]
    fn test_send_sync_embedding_provider() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockEmbeddingProvider>();

        let provider = MockEmbeddingProvider::builder().build();
        let _arc: Arc<dyn EmbeddingProvider> = Arc::new(provider);
    }

    // -- Delay tests -------------------------------------------------------

    #[tokio::test]
    async fn test_no_delay_when_options_are_none() {
        let provider = MockInferenceProvider::builder()
            .on_complete(a_completion_response("instant"), None)
            .build();

        let start = std::time::Instant::now();
        let resp = provider.complete(a_request("sys")).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(resp.text, "instant");
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "expected near-zero delay, got {elapsed:?}",
        );
    }

    #[tokio::test]
    async fn test_sequential_delay_applied() {
        let provider = MockInferenceProvider::builder()
            .on_complete(
                a_completion_response("delayed"),
                Some(MockProviderOptions {
                    delay: Some(std::time::Duration::from_millis(50)),
                }),
            )
            .build();

        let start = std::time::Instant::now();
        let resp = provider.complete(a_request("sys")).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(resp.text, "delayed");
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "expected >= 50ms delay, got {elapsed:?}",
        );
    }

    #[tokio::test]
    async fn test_conditional_delay_applied() {
        let provider = MockInferenceProvider::builder()
            .when(CompletionMatcher::system_contains("slow"))
            .respond_with(
                a_completion_response("slow-response"),
                Some(MockProviderOptions {
                    delay: Some(std::time::Duration::from_millis(50)),
                }),
            )
            .build();

        let start = std::time::Instant::now();
        let resp = provider.complete(a_request("slow")).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(resp.text, "slow-response");
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "expected >= 50ms delay, got {elapsed:?}",
        );
    }
}
