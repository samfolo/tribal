//! Filesystem watchers over a shared debounced loop.
//!
//! [`watch::watch_path`] is the generic watcher — a debounced `notify` watch on
//! a root, running a handler on each settled batch. The concrete watchers are
//! handlers over it: prompt hot-reload (validate, upsert, swap the active
//! version, publish `prompt.reloaded`) and config-change notification (publish
//! `config.changed` when the file is edited out from under the running server).

mod constants;
mod init;
mod reload;
mod self_write;
mod watch;

pub(crate) use init::{init_config_watcher, init_prompt_watcher};
pub(crate) use self_write::SelfWriteSentinel;
