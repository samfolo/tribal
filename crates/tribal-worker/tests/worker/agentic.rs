//! Integration tests for the agentic triage executor through the live
//! worker: the opt-in loop end to end, the budgeted one-shot, and the
//! ingest-claim divergence window's authority rule.

use std::collections::HashMap;

use async_trait::async_trait;
use tribal_agent_runtime::{BUDGET_RECHECK_CAUSE, SUBMIT_RESULT_TOOL, resolve_stage_thread};
use tribal_db::{
    AgentBindingVersionRepository, AgentThreadRecordRepository, AgentThreadRepository,
    ItemObservationRepository, PgAgentBindingVersionRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository, PgItemObservationRepository, PromptVersionRepository,
};
use tribal_domain::{
    AgentThreadRecordKind, AgentThreadStatus, AgentThreadSuspension, PromptClass, PromptRole,
    PromptStage, PromptVersionId, StageExecutorKind, TriageOutcome,
};
use tribal_test_utils::{
    a_new_knowledge_item, a_new_token_usage, a_tool_call_response, insert_embedding_for_profile,
};
use tribal_worker::{ActiveAgenticPrompts, StagePromptHashes, derive_stage_definition};

use super::common::*;

/// A fixed agentic prompt source over seeded version rows.
struct FixedAgenticPrompts(HashMap<(PromptStage, PromptClass, PromptRole), PromptVersionId>);

#[async_trait]
impl ActiveAgenticPrompts for FixedAgenticPrompts {
    async fn version_id(
        &self,
        stage: PromptStage,
        class: PromptClass,
        role: PromptRole,
    ) -> Option<PromptVersionId> {
        self.0.get(&(stage, class, role)).copied()
    }
}

/// Seeds the triage loop prompt pair and returns its source. The user
/// template renders the similar items' ids, so the membership corpus
/// genuinely contains what the model is told to copy.
async fn seed_loop_prompts(conn: &mut sqlx::PgConnection) -> FixedAgenticPrompts {
    let system = tribal_db::PgPromptVersionRepository
        .upsert(
            conn,
            &tribal_test_utils::a_new_prompt_version()
                .stage(PromptStage::Triage)
                .class(PromptClass::Loop)
                .role(PromptRole::System)
                .content("Investigate, then submit your decision.".to_owned())
                .content_hash("5".repeat(64))
                .build(),
        )
        .await
        .expect("seed loop system prompt");
    let user = tribal_db::PgPromptVersionRepository
        .upsert(
            conn,
            &tribal_test_utils::a_new_prompt_version()
                .stage(PromptStage::Triage)
                .class(PromptClass::Loop)
                .role(PromptRole::User)
                .content(
                    "candidate: {{ candidate.content }}\nsimilar:\n{% for item in similar_items %}\
                     - {{ item.item_id }}: {{ item.content }}\n{% endfor %}"
                        .to_owned(),
                )
                .content_hash("6".repeat(64))
                .build(),
        )
        .await
        .expect("seed loop user prompt");

    let mut slots = HashMap::new();
    slots.insert(
        (PromptStage::Triage, PromptClass::Loop, PromptRole::System),
        system.id(),
    );
    slots.insert(
        (PromptStage::Triage, PromptClass::Loop, PromptRole::User),
        user.id(),
    );
    FixedAgenticPrompts(slots)
}

/// The loop config: triage on the loop executor, verifier off (the
/// verifier lands with the driver family).
fn loop_agents_config() -> tribal_config::AgentsConfig {
    let mut agents = tribal_config::AgentsConfig::default();
    agents.triage.executor = tribal_config::ExecutorChoice::Loop;
    agents.triage.verifier = Some(false);
    agents
}

/// The loop executor completes a triage job end to end through the live
/// worker: the model submits a duplicate decision referencing an id from
/// its opening context, and the commit lands the observation, the typed
/// submission record, and the completed thread in one consistent shape.
#[tokio::test]
async fn test_the_loop_executor_completes_a_triage_job_end_to_end() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "agentic-loop").await;
    let mut conn = raw_conn(ctx).await;
    let prompts = seed_loop_prompts(&mut conn).await;
    let (job_id, task_id) = seed_triage_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
        &[a_candidate()
            .content("the loop's candidate".to_owned())
            .build()],
    )
    .await;

    // One existing claim, embedded under the active profile so the
    // opening search surfaces it.
    let profile = tribal_test_utils::active_embedding_profile(&mut conn).await;
    let existing = PgKnowledgeItemRepository
        .insert(
            &mut conn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("an existing claim".to_owned())
                .build(),
        )
        .await
        .expect("insert existing item");
    insert_embedding_for_profile(
        &mut conn,
        existing.id(),
        profile.id(),
        profile.model(),
        vec![0.5_f32; 768],
    )
    .await;
    drop(conn);

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_tool_call_response(&[(
                    "call_0",
                    SUBMIT_RESULT_TOOL,
                    serde_json::json!({
                        "decision": {
                            "decision": "duplicate",
                            "matched_item_id": existing.id().to_string(),
                        },
                        "considered_items": [{
                            "item_id": existing.id().to_string(),
                            "suggested_relation": "supports",
                            "justification": "restates the existing claim",
                        }],
                        "handoff": "the candidate matched an existing claim",
                    }),
                )]),
                None,
            )
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable("mock", "test stub")
            })))
            .build(),
    );
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.5_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );

    let token = CancellationToken::new();
    let (worker, _) = build_agentic_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        Some(embedding),
        loop_agents_config(),
        Arc::new(prompts),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;

    // The committed decision: an observation against the matched item.
    let triage_result = PgTriageResultRepository
        .find_by_job_id_and_batch_index(&mut conn, job_id, SEED_TRIAGE_BATCH_INDEX)
        .await
        .expect("find result")
        .expect("the loop committed a result");
    let TriageOutcome::Duplicate {
        matched_item_id, ..
    } = triage_result.outcome()
    else {
        panic!("a duplicate outcome, got {:?}", triage_result.outcome());
    };
    assert_eq!(*matched_item_id, existing.id());
    let observations = PgItemObservationRepository
        .find_by_knowledge_item_id(&mut conn, existing.id())
        .await
        .expect("observations");
    assert_eq!(observations.len(), 1);

    // The thread: completed under a loop binding, its log ending in the
    // typed submission with the handoff aboard.
    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find thread")
        .expect("present");
    assert_eq!(thread.status(), AgentThreadStatus::Completed);
    let binding = PgAgentBindingVersionRepository
        .find_by_id(&mut conn, thread.binding_version_id())
        .await
        .expect("find binding")
        .expect("present");
    assert_eq!(
        binding.definition().executor,
        StageExecutorKind::BuiltInLoop
    );
    assert!(
        binding
            .definition()
            .tools
            .iter()
            .any(|tool| tool.name == SUBMIT_RESULT_TOOL),
        "the binding hashes its tool surface",
    );

    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("log");
    let submission = records.last().expect("records");
    assert_eq!(submission.kind(), AgentThreadRecordKind::Submission);
    assert_eq!(
        submission.content()["payload"]["handoff"],
        "the candidate matched an existing claim",
        "the handoff rides the submission record for the relation stage",
    );
    let kinds: Vec<_> = records.iter().map(|r| r.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            AgentThreadRecordKind::Input,
            AgentThreadRecordKind::AssistantMessage,
            AgentThreadRecordKind::Submission,
        ],
        "one turn: the opening, the submit call, the accepted submission",
    );

    teardown(ctx).await;
}

/// A budgeted one-shot whose thread already carries ledger spend
/// suspends at the bracket before any call, and a wake under restored
/// headroom resumes it to completion — the shared admission contract on
/// the default executor.
#[tokio::test]
async fn test_a_budgeted_one_shot_suspends_pre_call_and_resumes() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "budgeted-one-shot").await;
    let mut conn = raw_conn(ctx).await;
    let (job_id, task_id) = seed_triage_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
        &[a_candidate().build()],
    )
    .await;

    // A prior attempt's spend: claim by hand, establish the thread,
    // attribute a ledger row to it, then requeue the task as a crash
    // would leave it.
    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "budget-seed")
        .await
        .expect("claim");
    let task = claimed.first().expect("claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition()
            .pipeline_stage(tribal_domain::TaskType::Triage)
            .build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    PgTokenUsageRepository
        .insert(
            &mut conn,
            &a_new_token_usage()
                .job_id(Some(job_id))
                .agent_thread_id(Some(stage_thread.thread.id()))
                .build(),
        )
        .await
        .expect("attribute prior spend");
    PgTaskRepository
        .requeue_claimed(
            &mut conn,
            task.id(),
            task.claim_token().expect("token"),
            0,
            chrono::Utc::now(),
            tribal_domain::TaskErrorKind::StartupReclaim,
            "test crash",
        )
        .await
        .expect("requeue");
    drop(conn);

    // Phase one: a worker whose budget the prior spend already exhausts
    // suspends the thread at the bracket — the opening embed is context
    // assembly, but no completion call runs (the inference mock panics
    // on any).
    let mut tight = loop_agents_config();
    tight.triage.executor = tribal_config::ExecutorChoice::OneShot;
    tight.triage.max_total_tokens = Some(1);
    let inference: Arc<dyn InferenceProvider> = Arc::new(MockInferenceProvider::builder().build());
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.5_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let token = CancellationToken::new();
    let (worker, _) = build_agentic_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        Some(embedding),
        tight,
        Arc::new(tribal_worker::NoAgenticPrompts),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };
    poll_task_status(&pool, task_id, TaskStatus::Blocked, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, stage_thread.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread.status(), AgentThreadStatus::Suspended);
    assert_eq!(
        thread.suspension(),
        Some(&AgentThreadSuspension::BudgetExhaustion {
            unchanged_rechecks: 0,
        }),
    );

    // Phase two: the wake, and a worker whose configuration restored
    // the headroom completes the same thread.
    let resolution = serde_json::json!({
        "cause": BUDGET_RECHECK_CAUSE,
        "fired_at": chrono::Utc::now(),
        "unchanged_rechecks": 0,
    });
    let woken = resolve_stage_thread(&mut conn, thread.id(), &resolution)
        .await
        .expect("resolve");
    assert!(matches!(woken, tribal_agent_runtime::ResolveOutcome::Woken));
    drop(conn);

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(super::fixtures::triage_novel_response_json()),
                None,
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.5_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        Some(embedding),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };
    poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(
        thread.status(),
        AgentThreadStatus::Completed,
        "headroom returning resumes the suspended one-shot to completion",
    );

    teardown(ctx).await;
}

/// The divergence window's authority rule: a job ingested under one
/// configuration and claimed under another runs the claim-time binding;
/// the fingerprint keeps naming ingest-time intent.
#[tokio::test]
async fn test_the_recorded_binding_wins_the_ingest_claim_divergence_window() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "divergence").await;
    let mut conn = raw_conn(ctx).await;
    let (job_id, task_id) = seed_triage_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
        &[a_candidate().build()],
    )
    .await;

    // The ingest-time intent: the fingerprint's triage hash names the
    // loop binding the configuration selected at ingest.
    let loop_hash = {
        let spec = test_stage_specs().triage;
        let definition = derive_stage_definition(
            tribal_domain::TaskType::Triage,
            &spec,
            &StagePromptHashes {
                system: "a".repeat(64),
                user: "b".repeat(64),
                loop_pair: Some(("5".repeat(64), "6".repeat(64))),
            },
            &loop_agents_config(),
        )
        .expect("derives");
        tribal_common::sha256_hex(&definition.canonical_json().expect("serialises"))
    };
    drop(conn);

    // The claim runs under today's default configuration: the worker
    // derives and records the one-shot binding, whatever the ingest
    // composite named.
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            .on_complete(
                a_completion_response(super::fixtures::triage_novel_response_json()),
                None,
            )
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let embedding: Arc<dyn EmbeddingProvider> = Arc::new(
        MockEmbeddingProvider::builder()
            .on_embed(an_embedding_response(vec![0.5_f32; 768]), None)
            .on_exhaust(ExhaustBehaviour::RepeatLast)
            .build(),
    );
    let token = CancellationToken::new();
    let worker = build_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        Some(embedding),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };
    poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    let _ = poll_job_status(&pool, job_id, JobStatus::Relating, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;

    let mut conn = raw_conn(ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find thread")
        .expect("present");
    let recorded = PgAgentBindingVersionRepository
        .find_by_id(&mut conn, thread.binding_version_id())
        .await
        .expect("find binding")
        .expect("present");

    assert_eq!(
        recorded.definition().executor,
        StageExecutorKind::OneShot,
        "execution followed the claim-time configuration",
    );
    assert_ne!(
        recorded.hash(),
        loop_hash,
        "the thread's recorded binding, not the ingest composite, is the truth of what ran",
    );

    teardown(ctx).await;
}
