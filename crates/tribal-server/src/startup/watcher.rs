//! Filesystem watchers over a shared debounced loop.
//!
//! [`watch::watch_path`] is the generic watcher — a debounced `notify` watch on
//! a root, running a handler on each settled batch. The concrete watchers are
//! handlers over it: prompt hot-reload (validate, upsert, and swap the active
//! version) and config-change observation for edits under the running server.

mod constants;
mod init;
mod reload;
mod watch;

pub(crate) use init::{init_config_watcher, init_prompt_watcher};
