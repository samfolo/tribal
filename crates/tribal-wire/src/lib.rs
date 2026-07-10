//! Wire DTOs for Tribal's client-facing surfaces. Each surface is its own
//! module — `mcp` today; an HTTP API or a desktop-client surface would be a
//! sibling module — so the contract has one source of truth and clients (the
//! eval harness, future SDKs) share exactly the types the server emits.
//!
//! The types are pure data: `serde` + `chrono` + `tribal-domain` only — never
//! the server, rmcp, or the database.

pub mod control;
pub mod gateway;
pub mod management;
pub mod mcp;

mod operator_check;
