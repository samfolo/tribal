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
pub(crate) use oauth::{expected_token_audience, resolve_oauth_runtime, runtime_data_plane};
pub(crate) use project::{resolve_project, resolve_project_mode};
pub(crate) use prompts::{
    PromptTemplateLocation, ensure_prompt_files, load_prompts, load_prompts_embedded,
};
pub(crate) use providers::{
    ProviderConnectionCredentialResolver, build_command_registry, build_provider_registry,
    completion_stage_specs, probe_startup_providers, validate_embedding_identity,
};
pub(crate) use provisioning::{provision_genesis, read_active_profile};
pub(crate) use watcher::{init_config_watcher, init_prompt_watcher};
