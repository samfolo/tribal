//! End-to-end coverage of the token-usage ledger.
//!
//! Every provider call the pipeline makes lands as exactly one attributed
//! `token_usage` row (job, task, stage, prompt versions), and a discover
//! query's embedding lands as an unowned query row. The exact row-set
//! assertions double as a tripwire against double-recording: a second
//! recording path would surface as a count mismatch here.

use serde_json::json;
use tribal_db::{PgTokenUsageRepository, TokenUsageRepository};
use tribal_domain::{EmbeddingPurpose, JobId, PipelineStage, TokenUsage};

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, novel, relate},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Counts the rows matching a `(stage, purpose)` shape.
fn count_of(rows: &[TokenUsage], stage: PipelineStage, purpose: Option<EmbeddingPurpose>) -> usize {
    rows.iter()
        .filter(|row| row.stage() == stage && row.purpose() == purpose)
        .count()
}

/// Theme: Canopy's build farm — two untagged facts, related by one edge.
/// Untagged candidates keep tag resolution out of the row set, so the
/// expected ledger shape is exact.
#[tokio::test]
async fn test_every_pipeline_call_ledgers_one_attributed_row() {
    let harness = TestHarness::init(|_setup| {}).await;

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(candidate(
                        "fact",
                        "The build farm caches intermediate artefacts for 14 days",
                    ))
                    .candidate(candidate(
                        "fact",
                        "Cache eviction on the build farm runs nightly at 02:00",
                    ))
                    .build(),
            );
        })
        .await;
    harness
        .mount_triage(|m| {
            m.respond(novel().build());
        })
        .await;
    harness
        .mount_relation(|m| {
            m.respond(
                RelationFixture::builder()
                    .edge(relate(0, "supports", 1))
                    .build(),
            );
        })
        .await;

    let ingest_result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "The build farm caches artefacts for 14 days; eviction \
                            runs nightly at 02:00."
            }),
        )
        .await;
    assert_success!(ingest_result);
    let job_id = tool_result_json(&ingest_result)["job_id"]
        .as_str()
        .expect("job_id in response")
        .parse::<JobId>()
        .expect("a well-formed job id");

    harness.expect_completion(&job_id.to_string()).await;

    // -- The pipeline's ledger rows, exactly ----------------------------------

    let mut conn = harness.pool.acquire().await.expect("ledger connection");
    let rows = PgTokenUsageRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find rows for the job");

    // One extraction completion, one candidate embedding and one triage
    // completion per candidate, one relation completion — and nothing else.
    assert_eq!(count_of(&rows, PipelineStage::Extraction, None), 1);
    assert_eq!(
        count_of(
            &rows,
            PipelineStage::Embedding,
            Some(EmbeddingPurpose::Candidate),
        ),
        2,
    );
    assert_eq!(count_of(&rows, PipelineStage::Triage, None), 2);
    assert_eq!(count_of(&rows, PipelineStage::Relation, None), 1);
    assert_eq!(rows.len(), 6, "no other rows attribute to the job");

    for row in &rows {
        assert_eq!(row.job_id(), Some(job_id));
        assert!(row.task_id().is_some(), "every pipeline row names its task");
        assert_eq!(row.attempt(), 0, "a first-attempt pipeline ledgers attempt 0");
        assert!(!row.provider().is_empty());
        assert!(!row.model().is_empty());

        let is_completion = row.stage() != PipelineStage::Embedding;
        assert_eq!(
            row.system_prompt_version_id().is_some(),
            is_completion,
            "prompt versions attach to completion rows alone (stage {})",
            row.stage(),
        );
        assert_eq!(row.user_prompt_version_id().is_some(), is_completion);
    }

    // -- A discover query's embedding ledgers as an unowned query row ---------

    let discover_result = harness
        .call_tool("tribal_discover", json!({ "query": "build farm cache" }))
        .await;
    assert_success!(discover_result);

    let all_rows = PgTokenUsageRepository
        .find_all_for_test(&mut conn)
        .await
        .expect("find all rows");
    let unowned: Vec<_> = all_rows.iter().filter(|r| r.job_id().is_none()).collect();

    assert_eq!(
        unowned
            .iter()
            .filter(|r| r.stage() == PipelineStage::Embedding
                && r.purpose() == Some(EmbeddingPurpose::Query))
            .count(),
        1,
        "the discover query embeds once, unowned",
    );
    assert!(
        unowned
            .iter()
            .all(|r| matches!(r.stage(), PipelineStage::Embedding | PipelineStage::Probe)),
        "unowned rows are only ever embeddings or probes",
    );
}
