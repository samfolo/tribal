//! Generic dispatch core shared by both mock providers.
//!
//! [`MockProviderCore`] handles sequential queue management, conditional
//! matching, call history recording, exhaustion behaviour, and diagnostic
//! tracing. It is generic over request and response types but hardcoded
//! to [`InferenceError`].

use std::{collections::VecDeque, fmt, sync::Mutex};

use tracing::debug;
use tribal_inference::{CompletionRequest, EmbeddingRequest, InferenceError};

use super::responses::ErrorFactory;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const MUTEX_POISONED: &str = "mock provider mutex poisoned";

// ---------------------------------------------------------------------------
// MockRequest trait
// ---------------------------------------------------------------------------

/// Extracts diagnostic information from request types for panic messages
/// and tracing.
pub(crate) trait MockRequest: Clone + Send + Sync + fmt::Debug {
    /// The model identifier from the request.
    fn model(&self) -> &str;

    /// The label for the context field shown in panic messages
    /// (`"system"` for completions, `"input"` for embeddings).
    fn context_label(&self) -> &'static str;

    /// The first 80 characters of the relevant context field, with
    /// `"..."` appended if truncated.
    fn context_preview(&self) -> String;
}

impl MockRequest for CompletionRequest {
    fn model(&self) -> &str {
        &self.model
    }

    fn context_label(&self) -> &'static str {
        "system"
    }

    fn context_preview(&self) -> String {
        match &self.system {
            Some(s) if s.len() > 80 => format!("{}...", &s[..80]),
            Some(s) => s.clone(),
            None => "<none>".to_owned(),
        }
    }
}

impl MockRequest for EmbeddingRequest {
    fn model(&self) -> &str {
        &self.model
    }

    fn context_label(&self) -> &'static str {
        "input"
    }

    fn context_preview(&self) -> String {
        if self.input.len() > 80 {
            format!("{}...", &self.input[..80])
        } else {
            self.input.clone()
        }
    }
}

// ---------------------------------------------------------------------------
// ExhaustBehaviour
// ---------------------------------------------------------------------------

/// Controls what happens when the sequential response queue is drained.
pub enum ExhaustBehaviour {
    /// Panic with diagnostic context (default).
    Panic,
    /// Clone and return the last successful sequential response
    /// indefinitely. Panics if no prior successful sequential response
    /// exists.
    RepeatLast,
    /// Invoke the error factory on each subsequent call.
    Error(ErrorFactory),
}

// ---------------------------------------------------------------------------
// Queue and conditional types
// ---------------------------------------------------------------------------

/// A single entry in the sequential response queue.
pub(crate) enum QueueEntry<Resp> {
    Ok(Resp),
    Err(ErrorFactory),
}

/// The outcome of a conditional match.
pub(crate) enum ConditionalOutcome<Resp> {
    Ok(Resp),
    Err(ErrorFactory),
}

/// A conditional entry: a predicate paired with a response or error.
pub(crate) struct ConditionalEntry<Req, Resp> {
    pub matcher: Box<dyn Fn(&Req) -> bool + Send + Sync>,
    pub outcome: ConditionalOutcome<Resp>,
}

// ---------------------------------------------------------------------------
// Core state
// ---------------------------------------------------------------------------

struct CoreState<Req, Resp> {
    queue: VecDeque<QueueEntry<Resp>>,
    last_ok_response: Option<Resp>,
    history: Vec<Req>,
    sequential_count: usize,
}

// ---------------------------------------------------------------------------
// MockProviderCore
// ---------------------------------------------------------------------------

/// Generic dispatch core for mock providers.
///
/// Handles sequential queue management, conditional matching, call
/// history recording, exhaustion behaviour, and diagnostic tracing.
pub(crate) struct MockProviderCore<Req: MockRequest, Resp: Clone + Send + Sync> {
    conditionals: Vec<ConditionalEntry<Req, Resp>>,
    exhaust_behaviour: ExhaustBehaviour,
    provider_name: &'static str,
    state: Mutex<CoreState<Req, Resp>>,
}

impl<Req: MockRequest, Resp: Clone + Send + Sync> MockProviderCore<Req, Resp> {
    /// Creates a new core with the given configuration.
    pub fn new(
        provider_name: &'static str,
        queue: VecDeque<QueueEntry<Resp>>,
        conditionals: Vec<ConditionalEntry<Req, Resp>>,
        exhaust_behaviour: ExhaustBehaviour,
    ) -> Self {
        let sequential_count = queue.len();
        Self {
            conditionals,
            exhaust_behaviour,
            provider_name,
            state: Mutex::new(CoreState {
                queue,
                last_ok_response: None,
                history: Vec::new(),
                sequential_count,
            }),
        }
    }

    /// Dispatches a request through the conditional → sequential →
    /// exhaustion resolution chain.
    pub fn dispatch(&self, request: &Req) -> Result<Resp, InferenceError> {
        let mut state = self.state.lock().expect(MUTEX_POISONED);

        state.history.push(request.clone());
        let call_number = state.history.len();

        // Check conditionals — first match wins.
        for (idx, entry) in self.conditionals.iter().enumerate() {
            if (entry.matcher)(request) {
                return match &entry.outcome {
                    ConditionalOutcome::Ok(resp) => {
                        let result = resp.clone();
                        debug!(
                            provider = self.provider_name,
                            call = call_number,
                            "conditional #{idx} matched"
                        );
                        Ok(result)
                    }
                    ConditionalOutcome::Err(factory) => {
                        let err = factory();
                        debug!(
                            provider = self.provider_name,
                            call = call_number,
                            "conditional #{idx} error"
                        );
                        Err(err)
                    }
                };
            }
        }

        // Pop from sequential queue.
        if let Some(entry) = state.queue.pop_front() {
            let consumed = state.sequential_count - state.queue.len();
            return match entry {
                QueueEntry::Ok(resp) => {
                    let result = resp.clone();
                    state.last_ok_response = Some(resp);
                    debug!(
                        provider = self.provider_name,
                        call = call_number,
                        "sequential queue pop ({consumed} of {total})",
                        total = state.sequential_count,
                    );
                    Ok(result)
                }
                QueueEntry::Err(factory) => {
                    let err = factory();
                    debug!(
                        provider = self.provider_name,
                        call = call_number,
                        "sequential queue pop ({consumed} of {total}), error",
                        total = state.sequential_count,
                    );
                    Err(err)
                }
            };
        }

        // Queue exhausted — apply exhaustion behaviour.
        self.handle_exhaustion(&mut state, request, call_number)
    }

    /// Returns the number of calls dispatched so far.
    pub fn call_count(&self) -> usize {
        self.state.lock().expect(MUTEX_POISONED).history.len()
    }

    /// Returns a clone of all requests dispatched so far, in order.
    pub fn history(&self) -> Vec<Req> {
        self.state.lock().expect(MUTEX_POISONED).history.clone()
    }

    /// Panics if the sequential queue has not been fully consumed.
    pub fn assert_exhausted(&self) {
        let state = self.state.lock().expect(MUTEX_POISONED);
        let remaining = state.queue.len();
        if remaining > 0 {
            let consumed = state.sequential_count - remaining;
            panic!(
                "{name}: {consumed} of {total} sequential responses consumed, \
                 {remaining} remaining",
                name = self.provider_name,
                total = state.sequential_count,
            );
        }
    }

    fn handle_exhaustion(
        &self,
        state: &mut CoreState<Req, Resp>,
        request: &Req,
        call_number: usize,
    ) -> Result<Resp, InferenceError> {
        let consumed = state.sequential_count;
        let conditionals_count = self.conditionals.len();

        match &self.exhaust_behaviour {
            ExhaustBehaviour::Panic => {
                panic!(
                    "{name}: sequential queue exhausted\n  \
                     sequential: {consumed} of {total} consumed, 0 remaining\n  \
                     conditionals: {conds} registered\n  \
                     call #{call}: model=\"{model}\", {label}=\"{preview}\"",
                    name = self.provider_name,
                    total = consumed,
                    conds = conditionals_count,
                    call = call_number,
                    model = request.model(),
                    label = request.context_label(),
                    preview = request.context_preview(),
                );
            }
            ExhaustBehaviour::RepeatLast => {
                let result = state.last_ok_response.clone().unwrap_or_else(|| {
                    panic!(
                        "{name}: RepeatLast exhaustion but no prior successful \
                         sequential response exists\n  \
                         sequential: {consumed} of {total} consumed\n  \
                         conditionals: {conds} registered\n  \
                         call #{call}: model=\"{model}\", {label}=\"{preview}\"",
                        name = self.provider_name,
                        total = consumed,
                        conds = conditionals_count,
                        call = call_number,
                        model = request.model(),
                        label = request.context_label(),
                        preview = request.context_preview(),
                    );
                });
                debug!(
                    provider = self.provider_name,
                    call = call_number,
                    "exhausted, repeating last"
                );
                Ok(result)
            }
            ExhaustBehaviour::Error(factory) => {
                let err = factory();
                debug!(
                    provider = self.provider_name,
                    call = call_number,
                    "exhaustion error"
                );
                Err(err)
            }
        }
    }
}
