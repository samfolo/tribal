//! Generic dispatch core shared by mock providers and mock repositories.
//!
//! [`MockAsyncDispatchCore`] handles sequential queue management, conditional
//! matching, call history recording, exhaustion behaviour, and diagnostic
//! tracing.  It is generic over request, response, **and** error types —
//! no domain-specific imports live here.

use std::{collections::VecDeque, fmt, sync::Mutex};

use tracing::debug;

use crate::text::truncate;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const MUTEX_POISONED: &str = "mock dispatch mutex poisoned";

// ---------------------------------------------------------------------------
// MockProviderOptions
// ---------------------------------------------------------------------------

/// Per-response options for mock dispatch entries.
#[derive(Debug, Clone, Default)]
pub struct MockProviderOptions {
    /// Delay applied before returning the response.
    pub delay: Option<std::time::Duration>,
}

// ---------------------------------------------------------------------------
// ErrorFactory
// ---------------------------------------------------------------------------

/// A boxed closure that produces a fresh error on each invocation.
pub type ErrorFactory<E> = Box<dyn Fn() -> E + Send + Sync>;

// ---------------------------------------------------------------------------
// ExhaustBehaviour
// ---------------------------------------------------------------------------

/// Controls what happens when the sequential response queue is drained.
pub enum ExhaustBehaviour<E> {
    /// Panic with diagnostic context (default).
    Panic,
    /// Clone and return the last successful sequential response
    /// indefinitely.  Panics if no prior successful sequential response
    /// exists.
    RepeatLast,
    /// Invoke the error factory on each subsequent call.
    Error(ErrorFactory<E>),
}

// ---------------------------------------------------------------------------
// Queue and conditional types
// ---------------------------------------------------------------------------

/// A single entry in the sequential response queue.
pub(crate) enum QueueEntry<Resp, Err> {
    Ok(Resp, Option<std::time::Duration>),
    Err(ErrorFactory<Err>, Option<std::time::Duration>),
}

/// The outcome of a conditional match.
pub(crate) enum ConditionalOutcome<Resp, Err> {
    Ok(Resp, Option<std::time::Duration>),
    Err(ErrorFactory<Err>, Option<std::time::Duration>),
}

/// A conditional entry: a predicate paired with a response or error.
pub(crate) struct ConditionalEntry<Req, Resp, Err> {
    pub matcher: Box<dyn Fn(&Req) -> bool + Send + Sync>,
    pub outcome: ConditionalOutcome<Resp, Err>,
}

// ---------------------------------------------------------------------------
// Core state
// ---------------------------------------------------------------------------

struct CoreState<Req, Resp, Err> {
    queue: VecDeque<QueueEntry<Resp, Err>>,
    last_ok_response: Option<Resp>,
    history: Vec<Req>,
    sequential_count: usize,
}

// ---------------------------------------------------------------------------
// MockAsyncDispatchCore
// ---------------------------------------------------------------------------

/// Generic dispatch core for mock providers and repositories.
///
/// Handles sequential queue management, conditional matching, call
/// history recording, exhaustion behaviour, and diagnostic tracing.
pub(crate) struct MockAsyncDispatchCore<Req, Resp, Err>
where
    Req: Clone + Send + Sync + fmt::Debug,
    Resp: Clone + Send + Sync,
    Err: fmt::Debug,
{
    conditionals: Vec<ConditionalEntry<Req, Resp, Err>>,
    exhaust_behaviour: ExhaustBehaviour<Err>,
    provider_name: &'static str,
    state: Mutex<CoreState<Req, Resp, Err>>,
}

impl<Req, Resp, Err> MockAsyncDispatchCore<Req, Resp, Err>
where
    Req: Clone + Send + Sync + fmt::Debug,
    Resp: Clone + Send + Sync,
    Err: fmt::Debug,
{
    /// Creates a new core with the given configuration.
    pub fn new(
        provider_name: &'static str,
        queue: VecDeque<QueueEntry<Resp, Err>>,
        conditionals: Vec<ConditionalEntry<Req, Resp, Err>>,
        exhaust_behaviour: ExhaustBehaviour<Err>,
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
    ///
    /// Returns the result alongside an optional delay that the caller
    /// should apply (via `tokio::time::sleep`) before returning the
    /// response.
    pub fn dispatch(&self, request: &Req) -> (Result<Resp, Err>, Option<std::time::Duration>) {
        let mut state = self.state.lock().expect(MUTEX_POISONED);

        state.history.push(request.clone());
        let call_number = state.history.len();

        // Check conditionals — first match wins.
        for (idx, entry) in self.conditionals.iter().enumerate() {
            if (entry.matcher)(request) {
                return match &entry.outcome {
                    ConditionalOutcome::Ok(resp, delay) => {
                        let result = resp.clone();
                        debug!(
                            provider = self.provider_name,
                            call = call_number,
                            "conditional #{idx} matched"
                        );
                        (Ok(result), *delay)
                    }
                    ConditionalOutcome::Err(factory, delay) => {
                        let err = factory();
                        debug!(
                            provider = self.provider_name,
                            call = call_number,
                            "conditional #{idx} error"
                        );
                        (Err(err), *delay)
                    }
                };
            }
        }

        // Pop from sequential queue.
        if let Some(entry) = state.queue.pop_front() {
            let consumed = state.sequential_count - state.queue.len();
            return match entry {
                QueueEntry::Ok(resp, delay) => {
                    let result = resp.clone();
                    state.last_ok_response = Some(resp);
                    debug!(
                        provider = self.provider_name,
                        call = call_number,
                        "sequential queue pop ({consumed} of {total})",
                        total = state.sequential_count,
                    );
                    (Ok(result), delay)
                }
                QueueEntry::Err(factory, delay) => {
                    let err = factory();
                    debug!(
                        provider = self.provider_name,
                        call = call_number,
                        "sequential queue pop ({consumed} of {total}), error",
                        total = state.sequential_count,
                    );
                    (Err(err), delay)
                }
            };
        }

        // Queue exhausted — apply exhaustion behaviour (no delay).
        (
            self.handle_exhaustion(&mut state, request, call_number),
            None,
        )
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
        state: &mut CoreState<Req, Resp, Err>,
        request: &Req,
        call_number: usize,
    ) -> Result<Resp, Err> {
        let consumed = state.sequential_count;
        let conditionals_count = self.conditionals.len();
        let request_preview = truncate(&format!("{request:?}"), 80);

        match &self.exhaust_behaviour {
            ExhaustBehaviour::Panic => {
                panic!(
                    "{name}: sequential queue exhausted\n  \
                     sequential: {consumed} of {total} consumed, 0 remaining\n  \
                     conditionals: {conds} registered\n  \
                     call #{call}: request={preview}",
                    name = self.provider_name,
                    total = consumed,
                    conds = conditionals_count,
                    call = call_number,
                    preview = request_preview,
                );
            }
            ExhaustBehaviour::RepeatLast => {
                let result = state.last_ok_response.clone().unwrap_or_else(|| {
                    panic!(
                        "{name}: RepeatLast exhaustion but no prior successful \
                         sequential response exists\n  \
                         sequential: {consumed} of {total} consumed\n  \
                         conditionals: {conds} registered\n  \
                         call #{call}: request={preview}",
                        name = self.provider_name,
                        total = consumed,
                        conds = conditionals_count,
                        call = call_number,
                        preview = request_preview,
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
