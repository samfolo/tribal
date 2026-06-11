//! The agent runtime: durable threads and the turn machinery.
//!
//! An agent run is a thread — an append-only sequence of records
//! committed to Postgres as they happen; the committed record is truth,
//! and resume, evaluation, and observability all derive from it. This
//! crate owns the thread store orchestration, the turn loop, the one-shot
//! executor, binding resolution, and the ledger-sink implementation. It
//! sits between `tribal-inference` and `tribal-worker`: the worker keeps
//! dispatch and stage assembly, delegating execution here, and nothing
//! below this crate depends back on it.

mod binding;
mod error;
mod ledger_sink;
mod store;
mod transitions;
mod turn;

pub use binding::resolve_binding;
pub use error::AgentRuntimeError;
pub use ledger_sink::PgLedgerSink;
pub use store::{StageThread, ensure_stage_thread};
pub use transitions::{
    CancelOutcome, ResolveOutcome, SuspendOutcome, cancel_unclaimed_thread, resolve_stage_thread,
    suspend_stage_thread,
};
pub use turn::{
    BegunTurn, OneShotOutcome, RecordedMessage, RenderedConversation, begin_one_shot,
    commit_noop_terminal, commit_one_shot_terminal,
};
