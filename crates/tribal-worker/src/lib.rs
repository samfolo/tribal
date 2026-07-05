#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Write-path pipeline for Tribal: task claiming, extraction, triage,
//! and relation execution, heartbeat management, and dead-lettering.

mod active_prompts;
mod common;
mod definition;
mod error;
mod gauge_task;
mod parsing;
mod prompt;
mod stages;
mod tag_resolution;
mod tools;
mod worker;

pub use active_prompts::{ActiveAgenticPrompts, NoAgenticPrompts};
pub use definition::{
    AgenticPromptHashes, DefinitionError, PromptHashPair, StagePromptHashes,
    derive_stage_definition, resolve_agentic_prompt_hashes,
};
pub use error::WorkerError;
pub use gauge_task::run_queue_health_gauges;
pub use prompt::{reserved_keys, synthetic_validation_context};
pub use worker::{
    MeteringTransport, ThreadReclaimStats, Worker, coupling,
    reindex::{ReindexCreationOutcome, ReindexTarget, create_reindex_run, resolve_reindex_target},
    reindex_ops::{
        ReindexCancelOutcome, ReindexOpError, ReindexPruneOutcome, ReindexResolution,
        ReindexRunOutcome, ReindexRunRequest, drop_superseded_indexes, reindex_cancel,
        reindex_prune, reindex_run,
    },
};
