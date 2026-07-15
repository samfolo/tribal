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
    ManagedConfig, ManagedRunError, ManagedRunOutcome, ManagedRuntime, MeteringTransport,
    ProbeSpec, ThreadReclaimStats, Worker, coupling, drive_managed_run,
    reindex::{ReindexCreationOutcome, ReindexTarget, create_reindex_run, resolve_reindex_target},
    reindex_ops::{
        PreparedReindexRun, ReindexCancelOutcome, ReindexOpError, ReindexPruneOutcome,
        ReindexResolution, ReindexRunOutcome, ReindexRunRequest, StagedReindexRun,
        drop_superseded_indexes, prepare_reindex_run, reindex_cancel, reindex_prune, reindex_run,
        stage_reindex_run,
    },
};
