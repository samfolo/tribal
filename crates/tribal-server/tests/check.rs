//! Integration tests for `tribal check`.
//!
//! Each test owns an isolated database via `TestDb` and uses scoped
//! guards for its env-var manipulation.

#[path = "check/common.rs"]
mod common;

#[path = "check/json_contract.rs"]
mod json_contract;
#[path = "check/orchestration.rs"]
mod orchestration;
#[path = "check/per_check.rs"]
mod per_check;
