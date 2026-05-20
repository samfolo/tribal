//! Integration tests for `tribal check`.
//!
//! Each test holds `serial_lock` for the duration of its env-var
//! manipulation and uses a fresh testcontainer-backed pool drained at
//! the start.

#[path = "check/common.rs"]
mod common;

#[path = "check/json_contract.rs"]
mod json_contract;
#[path = "check/orchestration.rs"]
mod orchestration;
#[path = "check/per_check.rs"]
mod per_check;
