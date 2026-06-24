use serde_json::json;
use tribal_config::ExecutorChoice;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// How long the verifier's response is held, so the parent's suspension
/// is observable as a blocked triage task. Comfortably longer than the
/// poll below, well within the task timeout (the driver child heartbeats
/// across the wait, and a blocked task is never claimed or reclaimed).
const VERIFIER_DELAY_MS: u64 = 1_200;

/// The full agentic triage pipeline over streamed wiremock: the loop runs
/// a read-tool turn and then a submission, both streamed (the loop's only
/// inference entry), and a fresh-context verifier child runs a buffered
/// one-shot on the same triage endpoint before the submission commits.
/// The two wire modes share the endpoint and are told apart by the
/// `stream` flag in the request body.
///
/// Theme: Canopy's deployment retry policy. One novel fact, verified and
/// committed through the loop.
#[tokio::test]
async fn test_agentic_triage_loop_end_to_end() {
    let mut harness = TestHarness::init(|setup| {
        setup.config(|config| {
            config.agents.triage.executor = ExecutorChoice::Loop;
            config.agents.triage.verifier = Some(true);
        });
    })
    .await;

    // -- Extraction: one novel candidate (one-shot, buffered) ----------------

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "fact",
                            "Canopy retries a failed canary deploy twice before paging \
                             the on-call engineer",
                        )
                        .tags(&["deployment", "canary"]),
                    )
                    .build(),
            );
        })
        .await;

    // -- Triage: the agentic loop (streamed) + the verifier (buffered) -------
    //
    // Turn one runs the candidate search the must-search rule requires
    // before any classification; turn two submits the novel decision. Both
    // are streamed. The verifier child's buffered verdict accepts the
    // submission, and the loop commits.

    harness
        .mount_triage(|m| {
            // The loop's two streamed turns hit one endpoint and are kept in
            // order by the single-use stubs, not by mutually exclusive
            // matchers: turn two also carries the stream flag, so it would
            // match here too were this stub not spent. Turn one (the opening)
            // is the first streamed request and consumes this single use.
            m.on_content_streamed(
                "\"stream\":true",
                &[json!({
                    "tool_calls": [{
                        "name": "search_candidate_similar_items",
                        "arguments": {},
                    }],
                })
                .into()],
            );
            // Turn two submits: with the stream-flag stub above spent, its
            // request falls through here on the tool result from turn one (a
            // `tool`-role message the opening lacks).
            m.on_content_streamed(
                "\"role\":\"tool\"",
                &[json!({
                    "tool_calls": [{
                        "name": "submit_result",
                        "arguments": {
                            "decision": { "decision": "created" },
                            "considered_items": [],
                            "handoff": "a novel deployment retry policy",
                        },
                    }],
                })
                .into()],
            );
            // The verifier child runs a buffered one-shot bearing the rubric;
            // it is the only buffered request to reach this stub (the startup
            // probe is absorbed by its own probe-input matcher). Its response
            // is delayed so the parent's suspension is observable: the triage
            // task blocks for the delay's duration.
            m.on_content_delayed(
                "verification agent",
                &[json!({ "accepted": true }).into()],
                VERIFIER_DELAY_MS,
            );
        })
        .await;

    // -- Relation: a single item has nothing to relate (one-shot, buffered) --

    harness
        .mount_relation(|m| {
            m.respond(RelationFixture::builder().build());
        })
        .await;

    // -- Ingest and run to completion ----------------------------------------

    let ingest = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "Canopy retries a failed canary deploy twice before paging the \
                            on-call engineer."
            }),
        )
        .await;
    assert_success!(ingest);
    let job_id = tool_result_json(&ingest)["job_id"]
        .as_str()
        .expect("job_id in response")
        .to_owned();

    // -- The verifier suspension blocks the triage task, live ----------------
    //
    // While the verifier's verdict is held, the parent's triage task sits
    // in the `blocked` status: the first task this codebase blocks in
    // production. `tribal_job_status` must report the job live through the
    // suspension, never terminal, and the blocked/queued cycle rides the
    // one task row exactly as the one-shot path's queued/claimed cycle does.

    let mut saw_blocked = false;
    for _ in 0..200 {
        let triage_status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM tasks WHERE task_type = 'triage' ORDER BY created_at DESC LIMIT 1",
        )
        .fetch_optional(&harness.pool)
        .await
        .expect("query the triage task status");

        if triage_status.as_deref() == Some("blocked") {
            saw_blocked = true;
            let status_result = harness
                .call_tool(
                    "tribal_job_status",
                    json!({ "job_id": job_id, "wait_seconds": 0 }),
                )
                .await;
            assert_success!(status_result);
            let reported = tool_result_json(&status_result)["status"]
                .as_str()
                .expect("job status field")
                .to_owned();
            assert!(
                reported != "completed" && reported != "failed",
                "tribal_job_status must report the job live while a triage thread is \
                 verifier-suspended, got '{reported}'",
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    assert!(
        saw_blocked,
        "the verifier suspension must block the triage task",
    );

    harness.expect_completion(&job_id).await;

    // -- The agentic-classified item is discoverable -------------------------

    let discover = harness
        .call_tool(
            "tribal_discover",
            json!({ "query": "canary deploy retry policy" }),
        )
        .await;
    assert_success!(discover);
    let discover_json = tool_result_json(&discover);
    let items = discover_json["items"].as_array().expect("items array");
    assert!(
        items.iter().any(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("retries a failed canary deploy"))
        }),
        "the item the agentic loop classified novel must be discoverable",
    );

    // -- The verifier ran on the triage endpoint, and the streamed turns -----
    //    repeat their prior conversation as a byte-identical prefix.

    let requests = harness
        .triage_server()
        .received_requests()
        .await
        .expect("recorded triage requests");
    let streamed: Vec<&wiremock::Request> = requests
        .iter()
        .filter(|r| String::from_utf8_lossy(&r.body).contains("\"stream\":true"))
        .collect();
    // The verifier child is the buffered request bearing the rubric; the
    // startup provider probe is also buffered but carries no rubric.
    let verifier_calls = requests
        .iter()
        .filter(|r| String::from_utf8_lossy(&r.body).contains("verification agent"))
        .count();
    assert_eq!(
        streamed.len(),
        2,
        "the loop ran two streamed turns: the candidate search and the submission",
    );
    assert_eq!(
        verifier_calls, 1,
        "the verifier child ran one buffered one-shot on the same endpoint",
    );

    let turn_messages = |request: &wiremock::Request| -> Vec<serde_json::Value> {
        let body: serde_json::Value =
            serde_json::from_slice(&request.body).expect("triage request body is JSON");
        body["messages"].as_array().cloned().unwrap_or_default()
    };
    let first = turn_messages(streamed[0]);
    let second = turn_messages(streamed[1]);
    assert!(
        second.len() > first.len(),
        "the second turn extends the conversation with the tool exchange",
    );
    // The second turn's conversation extends the first's without rewriting
    // it: turn one's messages are an exact prefix of turn two's. This is
    // the append-only property the prompt-cache prefix rides on; the
    // verbatim byte-stability of the replay itself is the runtime
    // invariant exercised at the one-shot and inference tiers.
    assert_eq!(
        &second[..first.len()],
        &first[..],
        "the prior turn's conversation is an exact prefix of the next request",
    );

    // -- The candidate is embedded at its two consumption points only --------
    //
    // The search tool embeds the candidate once on turn one, reusing that
    // vector across any further pages; the second embedding is deferred to
    // the novel commit, where the vector is finally consumed, derived
    // against the then-active profile so the committed vector and its profile
    // id stay consistent rather than a recorded vector being replayed under a
    // profile it was not built under. So for this verified novel fact the
    // embedding endpoint sees the candidate exactly twice, the candidate
    // search and the novel commit.
    let candidate_embeds = harness
        .embedding_server()
        .received_requests()
        .await
        .expect("recorded embedding requests")
        .iter()
        .filter(|r| String::from_utf8_lossy(&r.body).contains("retries a failed canary deploy"))
        .count();
    assert_eq!(
        candidate_embeds, 2,
        "the candidate is embedded for the candidate search and again at the novel commit, \
         never replayed from a recorded vector",
    );

    harness.shutdown().await;
}
