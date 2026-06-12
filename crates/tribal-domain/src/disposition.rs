//! The disposition decision: one function, every actor.
//!
//! Both counters, both reset rules, and the thread-to-task terminal
//! mapping live here and nowhere else — never spread across SQL CASE
//! expressions. The task's `retry_count` bounds *consecutive* failures
//! and resets at the start of every recovery cycle (and, once an
//! executor commits records mid-thread, on any successful commit); the
//! thread's `recovery_attempts` accumulates across cycles, never resets,
//! and drives the escalating per-cycle backoff. The
//! driving task never transits dead-letter on the way to a fresh recovery
//! cycle: dead-letter fires irreversible job couplings and is reserved for
//! terminal outcomes.

use crate::AgentThreadTerminal;

/// The consecutive-failure count after a successful record commit: any
/// progress resets it. One-shot execution has no progress point between
/// failures, so the rule's only live application is the cycle-start
/// reset below; the loop executor's record commits activate the rest.
pub const RETRY_COUNT_AFTER_PROGRESS: u32 = 0;

/// What an actor observed at the end of a turn or boundary check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnOutcome {
    /// The thread reached a terminal status.
    ThreadTerminal {
        /// The terminal status reached.
        terminal: AgentThreadTerminal,
    },
    /// The turn failed retryably; the thread stays live.
    RetryableFailure,
}

/// The counters the decision reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispositionCounters {
    /// Consecutive failures so far, before this failure is counted.
    pub retry_count: u32,
    /// The consecutive-failure budget; exceeded strictly after increment.
    pub max_retries: u32,
    /// Recovery cycles consumed so far.
    pub recovery_attempts: u32,
    /// The recovery-cycle budget; exceeded strictly after increment.
    pub max_recovery_attempts: u32,
}

/// What happens to the driving task (and, on exhaustion, the thread).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// The thread completed: complete the driving task.
    CompleteTask,
    /// The thread ended `failed`, `cancelled`, or `dead_letter`:
    /// dead-letter the driving task. A cancelled stage deliberately reads
    /// as a failed task on the launched surface.
    DeadLetterTask,
    /// Retry budget remains: re-queue the same row with backoff.
    Requeue {
        /// The incremented consecutive-failure count to persist.
        retry_count: u32,
    },
    /// Retry budget exhausted, recovery budget remains: one claim-guarded
    /// transaction re-queues the same row with the consecutive counter
    /// reset and the cycle counter advanced; per-cycle backoff escalates
    /// with the cycle count.
    RecoveryCycle {
        /// The reset consecutive-failure count (cycle-start rule).
        retry_count: u32,
        /// The incremented recovery-cycle count to persist.
        recovery_attempts: u32,
    },
    /// Both budgets exhausted: the thread dead-letters and the driving
    /// task dead-letters with it.
    ExhaustThread,
}

/// Decides the disposition for one observed outcome.
///
/// The whole decision — both counters, both reset rules, the terminal
/// mapping — so SQL never re-encodes any part of it.
#[must_use]
pub fn decide_disposition(outcome: TurnOutcome, counters: DispositionCounters) -> Disposition {
    match outcome {
        TurnOutcome::ThreadTerminal {
            terminal: AgentThreadTerminal::Completed,
        } => Disposition::CompleteTask,
        TurnOutcome::ThreadTerminal { .. } => Disposition::DeadLetterTask,
        TurnOutcome::RetryableFailure => {
            let failures = counters.retry_count + 1;
            if failures <= counters.max_retries {
                return Disposition::Requeue {
                    retry_count: failures,
                };
            }
            let cycles = counters.recovery_attempts + 1;
            if cycles <= counters.max_recovery_attempts {
                return Disposition::RecoveryCycle {
                    retry_count: RETRY_COUNT_AFTER_PROGRESS,
                    recovery_attempts: cycles,
                };
            }
            Disposition::ExhaustThread
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counters(retry: u32, recovery: u32) -> DispositionCounters {
        DispositionCounters {
            retry_count: retry,
            max_retries: 3,
            recovery_attempts: recovery,
            max_recovery_attempts: 2,
        }
    }

    #[test]
    fn test_completed_thread_completes_the_task() {
        let disposition = decide_disposition(
            TurnOutcome::ThreadTerminal {
                terminal: AgentThreadTerminal::Completed,
            },
            counters(0, 0),
        );
        assert!(matches!(disposition, Disposition::CompleteTask));
    }

    #[test]
    fn test_every_non_completed_terminal_dead_letters_the_task() {
        for terminal in [
            AgentThreadTerminal::Failed,
            AgentThreadTerminal::Cancelled,
            AgentThreadTerminal::DeadLetter,
        ] {
            let disposition =
                decide_disposition(TurnOutcome::ThreadTerminal { terminal }, counters(0, 0));
            assert!(
                matches!(disposition, Disposition::DeadLetterTask),
                "{terminal:?} must dead-letter the task",
            );
        }
    }

    #[test]
    fn test_retry_budget_requeues_with_incremented_count() {
        let disposition = decide_disposition(TurnOutcome::RetryableFailure, counters(1, 0));
        assert!(matches!(
            disposition,
            Disposition::Requeue { retry_count: 2 },
        ));
    }

    #[test]
    fn test_exhausted_retries_open_a_recovery_cycle_with_reset_counter() {
        // retry_count 3 with max 3: the fourth consecutive failure
        // exhausts the budget (strictly-greater-after-increment, the
        // launched convention) and opens cycle one.
        let disposition = decide_disposition(TurnOutcome::RetryableFailure, counters(3, 0));
        assert!(matches!(
            disposition,
            Disposition::RecoveryCycle {
                retry_count: RETRY_COUNT_AFTER_PROGRESS,
                recovery_attempts: 1,
            },
        ));
    }

    #[test]
    fn test_recovery_cycle_never_dead_letters_the_task() {
        // Same boundary, every remaining cycle: the disposition is a
        // cycle, never a task dead-letter.
        let disposition = decide_disposition(TurnOutcome::RetryableFailure, counters(3, 1));
        assert!(matches!(
            disposition,
            Disposition::RecoveryCycle {
                recovery_attempts: 2,
                ..
            },
        ));
    }

    #[test]
    fn test_exhausted_recovery_dead_letters_the_thread() {
        let disposition = decide_disposition(TurnOutcome::RetryableFailure, counters(3, 2));
        assert!(matches!(disposition, Disposition::ExhaustThread));
    }
}
