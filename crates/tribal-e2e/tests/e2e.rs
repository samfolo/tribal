mod harness;

// Test modules — each file exercises a different cross-cutting E2E flow.
// All tests share a single testcontainer via `serial_lock`, ensuring
// sequential execution within this binary.
#[path = "e2e/concurrent_ingests.rs"]
mod concurrent_ingests;
#[path = "e2e/cross_batch_relations.rs"]
mod cross_batch_relations;
#[path = "e2e/duplicate_batch.rs"]
mod duplicate_batch;
#[path = "e2e/explore.rs"]
mod explore;
#[path = "e2e/feedback.rs"]
mod feedback;
#[path = "e2e/ingest_flow.rs"]
mod ingest_flow;
#[path = "e2e/multi_session.rs"]
mod multi_session;
#[path = "e2e/provider_retry.rs"]
mod provider_retry;
#[path = "e2e/session.rs"]
mod session;
#[path = "e2e/standing.rs"]
mod standing;
#[path = "e2e/zero_candidate.rs"]
mod zero_candidate;
