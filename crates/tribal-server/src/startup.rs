//! Infrastructure bootstrap for the `tribal serve` command.
//!
//! Submodules implement each phase of the startup sequence: tracing
//! initialisation, database connection, migration, prompt loading,
//! provider construction, and project resolution.

mod constants;
mod database;
mod instance_id;
mod migration;
mod oauth;
mod project;
mod prompts;
mod providers;
mod provisioning;
mod watcher;

pub(crate) use constants::{POOL_NAME_MCP, POOL_NAME_WORKER};
pub(crate) use database::create_pool_with_retry;
pub(crate) use instance_id::generate_instance_id;
pub(crate) use migration::{check_first_run, run_migrations};
pub(crate) use oauth::{expected_token_audience, resolve_oauth_runtime};
pub(crate) use project::resolve_project;
pub(crate) use prompts::{
    PromptTemplateLocation, ensure_prompt_files, load_prompts, load_prompts_embedded,
};
pub(crate) use providers::{
    build_embedding_provider, build_inference_provider, build_provider_registry,
    probe_embedding_provider, probe_inference_provider,
};
pub(crate) use provisioning::provision_genesis;
pub(crate) use watcher::init_prompt_watcher;
