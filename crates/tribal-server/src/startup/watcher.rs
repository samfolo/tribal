//! File watcher for prompt hot-reload.
//!
//! Monitors the prompts directory for changes, validates modified
//! templates against the production context shape, upserts new versions
//! into the database, and atomically swaps the in-memory active prompt
//! IDs.

mod constants;
mod init;
mod reload;

pub(crate) use init::init_prompt_watcher;
