#![warn(clippy::pedantic)]
#![allow(clippy::must_use_candidate)]
#![deny(warnings)]
//! The agent runtime: durable threads and the turn machinery.
//!
//! An agent run is a thread: an append-only sequence of records
//! committed to Postgres as they happen; the committed record is truth,
//! and resume, evaluation, and observability all derive from it. This
//! crate owns the thread store orchestration, the one-shot turn bracket,
//! the guarded transitions, binding resolution, and the ledger-sink
//! implementation. It sits between `tribal-inference` and
//! `tribal-worker`: the worker keeps
//! dispatch and stage assembly, delegating execution here, and nothing
//! below this crate depends back on it.

mod binding;
mod driver;
mod error;
mod ledger_sink;
mod store;
mod tools;
mod transitions;
mod turn;
mod turn_loop;
mod txn;

pub use binding::resolve_binding;
pub use driver::{
    ChildLaunch, ChildTerminalOutcome, LaunchedChild, ParentResolution, SuspendWithChildOutcome,
    commit_child_terminal, commit_deferred_death, suspend_with_child,
};
pub use error::AgentRuntimeError;
pub use ledger_sink::PgLedgerSink;
pub use store::{StageThread, ensure_stage_thread};
pub use tools::{StageTool, ToolOutcome, ToolRegistry, ToolRegistryError};
pub use transitions::{
    CancelOutcome, ResolveOutcome, SuspendOutcome, cancel_thread_in_txn, resolve_stage_thread,
    suspend_stage_thread,
};
pub use turn::{
    BegunTurn, DrivingClaim, OneShotOutcome, RecordedMessage, RecordedToolCall,
    RenderedConversation, adopt_conversation, begin_turn, commit_noop_terminal,
    commit_one_shot_terminal, recorded_conversation,
};
pub use turn_loop::{
    AcceptedSubmission, Admission, AdmissionDecision, BUDGET_RECHECK_CAUSE, BudgetFailure,
    HeartbeatPump, LoopOutcome, RecheckPolicy, SUBMIT_RESULT_TOOL, SeenCorpus, SubmissionContent,
    SubmissionOutcome, SubmissionPipeline, ToolResultContent, TurnLoopDependencies, VerdictContent,
    VerifierLaunch, admit_inference, carried_rechecks, commit_loop_terminal, decide_admission,
    run_turn_loop, verdict_schema,
};
