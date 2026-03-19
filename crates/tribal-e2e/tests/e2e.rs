mod harness;

// Test modules — each file exercises a different cross-cutting E2E flow.
// All tests share a single testcontainer via `serial_lock`, ensuring
// sequential execution within this binary.
#[path = "e2e/explore.rs"]
mod explore;
#[path = "e2e/ingest_flow.rs"]
mod ingest_flow;
#[path = "e2e/session.rs"]
mod session;
