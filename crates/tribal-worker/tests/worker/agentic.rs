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
use tribal_worker::{
    ActiveAgenticPrompts, AgenticPromptHashes, StagePromptHashes, derive_stage_definition,
};

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

/// Seeds the triage loop prompt pair and returns its source. The opening
/// pre-loads no similar items: the loop reaches the corpus through its
/// search tool, so the model's referenced ids come from a tool result, not
/// the rendered opening.
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
                .content("candidate: {{ candidate.content }}".to_owned())
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
    agents.triage.verifier = Some(tribal_config::VerifierConfig { enabled: false });
    agents
}

/// The loop config for relation: relation on the loop executor, triage
/// left one-shot so the seeded job reaches an agentic relation stage.
fn relation_loop_agents_config() -> tribal_config::AgentsConfig {
    let mut agents = tribal_config::AgentsConfig::default();
    agents.relation.executor = tribal_config::ExecutorChoice::Loop;
    agents
}

/// The loop config for extraction: extraction on the loop executor.
fn extraction_loop_agents_config() -> tribal_config::AgentsConfig {
    let mut agents = tribal_config::AgentsConfig::default();
    agents.extraction.executor = tribal_config::ExecutorChoice::Loop;
    agents
}

/// Seeds the extraction loop prompt pair and returns its source. The user
/// template renders the raw input the model extracts from.
async fn seed_extraction_loop_prompts(conn: &mut sqlx::PgConnection) -> FixedAgenticPrompts {
    let system = tribal_db::PgPromptVersionRepository
        .upsert(
            conn,
            &tribal_test_utils::a_new_prompt_version()
                .stage(PromptStage::Extraction)
                .class(PromptClass::Loop)
                .role(PromptRole::System)
                .content("Extract the claims, then submit.".to_owned())
                .content_hash("3".repeat(64))
                .build(),
        )
        .await
        .expect("seed extraction loop system prompt");
    let user = tribal_db::PgPromptVersionRepository
        .upsert(
            conn,
            &tribal_test_utils::a_new_prompt_version()
                .stage(PromptStage::Extraction)
                .class(PromptClass::Loop)
                .role(PromptRole::User)
                .content("input: {{ raw_input }}".to_owned())
                .content_hash("4".repeat(64))
                .build(),
        )
        .await
        .expect("seed extraction loop user prompt");

    let mut slots = HashMap::new();
    slots.insert(
        (
            PromptStage::Extraction,
            PromptClass::Loop,
            PromptRole::System,
        ),
        system.id(),
    );
    slots.insert(
        (PromptStage::Extraction, PromptClass::Loop, PromptRole::User),
        user.id(),
    );
    FixedAgenticPrompts(slots)
}

/// Seeds the relation loop prompt pair and returns its source. The user
/// template renders each committed item's id, so the membership corpus
/// genuinely contains the ids the model copies into its edges.
async fn seed_relation_loop_prompts(conn: &mut sqlx::PgConnection) -> FixedAgenticPrompts {
    let system = tribal_db::PgPromptVersionRepository
        .upsert(
            conn,
            &tribal_test_utils::a_new_prompt_version()
                .stage(PromptStage::Relation)
                .class(PromptClass::Loop)
                .role(PromptRole::System)
                .content("Investigate, then submit the edges.".to_owned())
                .content_hash("7".repeat(64))
                .build(),
        )
        .await
        .expect("seed relation loop system prompt");
    let user = tribal_db::PgPromptVersionRepository
        .upsert(
            conn,
            &tribal_test_utils::a_new_prompt_version()
                .stage(PromptStage::Relation)
                .class(PromptClass::Loop)
                .role(PromptRole::User)
                .content(
                    "batch:\n{% for c in candidates %}- {{ c.item_id }}: {{ c.candidate.content }}\n\
                     {% endfor %}"
                        .to_owned(),
                )
                .content_hash("8".repeat(64))
                .build(),
        )
        .await
        .expect("seed relation loop user prompt");

    let mut slots = HashMap::new();
    slots.insert(
        (PromptStage::Relation, PromptClass::Loop, PromptRole::System),
        system.id(),
    );
    slots.insert(
        (PromptStage::Relation, PromptClass::Loop, PromptRole::User),
        user.id(),
    );
    FixedAgenticPrompts(slots)
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
            // Turn one: the candidate search the must-search rule requires
            // before any classification can be cited.
            .on_complete(
                a_tool_call_response(&[(
                    "call_0",
                    "search_candidate_similar_items",
                    serde_json::json!({}),
                )]),
                None,
            )
            // Turn two: the submission, now grounded in the search result.
            .on_complete(
                a_tool_call_response(&[(
                    "call_1",
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
            .any(|tool| tool.descriptor.name == SUBMIT_RESULT_TOOL),
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
            AgentThreadRecordKind::ToolResult,
            AgentThreadRecordKind::AssistantMessage,
            AgentThreadRecordKind::Submission,
        ],
        "two turns: the opening, the candidate search and its result, then \
         the submit call and the accepted submission",
    );

    teardown(ctx).await;
}

/// A budgeted one-shot whose thread already carries ledger spend
/// suspends at the bracket before any call, and a wake under restored
/// headroom resumes it to completion: the shared admission contract on
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
        &a_routed_definition(tribal_domain::TaskType::Triage),
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
    // suspends the thread at the bracket. The opening embed is context
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
                agentic: AgenticPromptHashes {
                    loop_pair: Some(("5".repeat(64), "6".repeat(64))),
                    verifier_pair: None,
                },
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

/// The loop executor completes a relation job end to end through the live
/// worker: the model runs a cross-project search, then submits one edge
/// between two committed batch items by their ids, and the commit lands the
/// relation, seals the batch, and transitions the job to completed.
#[tokio::test]
async fn test_the_loop_executor_completes_a_relation_job_end_to_end() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "agentic-relation-loop").await;
    let mut conn = raw_conn(ctx).await;
    let prompts = seed_relation_loop_prompts(&mut conn).await;
    let candidates = vec![
        a_candidate()
            .content("Rust has zero-cost abstractions".to_owned())
            .build(),
        a_candidate()
            .content("Ownership prevents data races".to_owned())
            .build(),
    ];
    let (job_id, task_id, ki_ids) = seed_relation_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
        &candidates,
        &[a_relation_hint().build()],
    )
    .await;
    drop(conn);

    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            // Turn one: a cross-project search for related claims.
            .on_complete(
                a_tool_call_response(&[(
                    "call_0",
                    "search_related_items",
                    serde_json::json!({ "query": "memory safety guarantees" }),
                )]),
                None,
            )
            // Turn two: one edge between the two committed batch items.
            .on_complete(
                a_tool_call_response(&[(
                    "call_1",
                    SUBMIT_RESULT_TOOL,
                    serde_json::json!({
                        "relations": [{
                            "source_id": ki_ids[0].to_string(),
                            "target_id": ki_ids[1].to_string(),
                            "relation_type": "supports",
                            "justification": "the first reinforces the second",
                        }],
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
        relation_loop_agents_config(),
        Arc::new(prompts),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let (task, job) = tokio::join!(
        poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE),
        poll_job_status(&pool, job_id, JobStatus::Completed, POLL_SETTLE),
    );
    token.cancel();
    let _ = handle.await;

    assert_eq!(task.status(), TaskStatus::Completed);
    assert_eq!(job.status(), JobStatus::Completed);
    assert!(
        job.committed_batch_id().is_some(),
        "the loop sealed the relation batch",
    );

    let mut conn = raw_conn(ctx).await;

    // The submitted edge committed between the two batch items.
    let outbound = PgRelationRepository
        .find_outbound(&mut conn, ki_ids[0], None)
        .await
        .expect("find outbound relations");
    assert_eq!(outbound.len(), 1, "the one submitted edge committed");
    assert_eq!(outbound[0].target_id(), ki_ids[1]);
    assert_eq!(
        outbound[0].justification(),
        Some("the first reinforces the second"),
    );

    // The thread completed under a loop binding, its log ending in the
    // accepted submission.
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
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("log");
    assert_eq!(
        records.last().expect("records").kind(),
        AgentThreadRecordKind::Submission,
        "the log ends in the accepted submission",
    );

    teardown(ctx).await;
}

/// The loop executor completes an extraction job end to end through the
/// live worker: the model submits two candidates in one turn, and the
/// commit persists the extraction result and fans out one triage task per
/// candidate, exactly as the one-shot path would.
#[tokio::test]
async fn test_the_loop_executor_completes_an_extraction_job_end_to_end() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "agentic-extraction-loop").await;
    let mut conn = raw_conn(ctx).await;
    let prompts = seed_extraction_loop_prompts(&mut conn).await;
    let (job_id, task_id) = seed_extraction_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
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
                        "candidates": [
                            {"kind": "fact", "content": "first claim", "suggested_tags": []},
                            {"kind": "fact", "content": "second claim", "suggested_tags": []},
                        ],
                        "relation_hints": [],
                    }),
                )]),
                None,
            )
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable("mock", "test stub")
            })))
            .build(),
    );

    let token = CancellationToken::new();
    let (worker, _) = build_agentic_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
        extraction_loop_agents_config(),
        Arc::new(prompts),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    // The extraction task completes and the job moves to triaging: the
    // fan-out the loop commit performed.
    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    let _ = poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;
    assert_eq!(task.status(), TaskStatus::Completed);

    let mut conn = raw_conn(ctx).await;

    // The extraction result carries the two submitted candidates.
    let result = PgExtractionResultRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find extraction result")
        .expect("the loop committed an extraction result");
    let candidates: Vec<serde_json::Value> =
        serde_json::from_value(result.candidates().clone()).expect("candidates are an array");
    assert_eq!(candidates.len(), 2, "both submitted candidates committed");

    // The loop fanned out one triage task per candidate: the central
    // committed effect, asserted directly rather than inferred from the
    // job's transition to triaging.
    let tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find the job's tasks");
    let triage_tasks = tasks
        .iter()
        .filter(|t| t.task_type() == tribal_domain::TaskType::Triage)
        .count();
    assert_eq!(triage_tasks, 2, "one triage task fanned out per candidate");

    // The thread completed under a loop binding, its log ending in the
    // accepted submission.
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

    teardown(ctx).await;
}

/// A deterministic validator bounce drives a second turn before fan-out:
/// the first submission carries an out-of-range relation hint and bounces
/// with diagnostics, the loop re-prompts, and the corrected resubmission
/// commits and fans out one triage task per candidate. The bounce never
/// reaches the database; only the accepted submission does.
#[tokio::test]
async fn test_a_bounced_extraction_resubmits_then_fans_out() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create pool");

    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, "agentic-extraction-bounce").await;
    let mut conn = raw_conn(ctx).await;
    let prompts = seed_extraction_loop_prompts(&mut conn).await;
    let (job_id, task_id) = seed_extraction_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
    )
    .await;
    drop(conn);

    let candidate = |content: &str| serde_json::json!({"kind": "fact", "content": content, "suggested_tags": []});
    let inference: Arc<dyn InferenceProvider> = Arc::new(
        MockInferenceProvider::builder()
            // Turn one: a single candidate with a relation hint pointing at a
            // candidate that does not exist; the validator bounces it.
            .on_complete(
                a_tool_call_response(&[(
                    "call_0",
                    SUBMIT_RESULT_TOOL,
                    serde_json::json!({
                        "candidates": [candidate("first claim")],
                        "relation_hints": [{
                            "source_index": 0,
                            "target_index": 5,
                            "hint_type": "derived_from",
                        }],
                    }),
                )]),
                None,
            )
            // Turn two: the corrected submission, two candidates and no
            // dangling hint, which the validator accepts.
            .on_complete(
                a_tool_call_response(&[(
                    "call_1",
                    SUBMIT_RESULT_TOOL,
                    serde_json::json!({
                        "candidates": [candidate("first claim"), candidate("second claim")],
                        "relation_hints": [],
                    }),
                )]),
                None,
            )
            .on_exhaust(ExhaustBehaviour::Error(Box::new(|| {
                tribal_inference::InferenceError::provider_unavailable("mock", "test stub")
            })))
            .build(),
    );

    let token = CancellationToken::new();
    let (worker, _) = build_agentic_test_worker(
        pool.clone(),
        token.clone(),
        test_config(),
        Some(inference),
        None,
        extraction_loop_agents_config(),
        Arc::new(prompts),
    )
    .await;
    let handle = {
        let w = Arc::clone(&worker);
        tokio::spawn(async move { w.run().await })
    };

    let task = poll_task_status(&pool, task_id, TaskStatus::Completed, POLL_SETTLE).await;
    let _ = poll_job_status(&pool, job_id, JobStatus::Triaging, POLL_SETTLE).await;
    token.cancel();
    let _ = handle.await;
    assert_eq!(task.status(), TaskStatus::Completed);

    let mut conn = raw_conn(ctx).await;

    // Only the accepted resubmission committed: two candidates, two triage
    // tasks, never the bounced single-candidate submission.
    let result = PgExtractionResultRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find extraction result")
        .expect("the loop committed an extraction result");
    let candidates: Vec<serde_json::Value> =
        serde_json::from_value(result.candidates().clone()).expect("candidates are an array");
    assert_eq!(candidates.len(), 2, "the corrected submission committed");
    let triage_tasks = PgTaskRepository
        .find_by_job_id(&mut conn, job_id)
        .await
        .expect("find the job's tasks")
        .into_iter()
        .filter(|t| t.task_type() == tribal_domain::TaskType::Triage)
        .count();
    assert_eq!(
        triage_tasks, 2,
        "one triage task fanned out per accepted candidate"
    );

    // The log records the bounce as an error tool result between the two
    // submit calls: a turn the validator drove without a database write.
    let thread = PgAgentThreadRepository
        .find_by_stage_task_id(&mut conn, task_id)
        .await
        .expect("find thread")
        .expect("present");
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(&mut conn, thread.id())
        .await
        .expect("log");
    let kinds: Vec<_> = records.iter().map(|r| r.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            AgentThreadRecordKind::Input,
            AgentThreadRecordKind::AssistantMessage,
            AgentThreadRecordKind::ToolResult,
            AgentThreadRecordKind::AssistantMessage,
            AgentThreadRecordKind::Submission,
        ],
        "the bounce is a tool result, then the corrected submit is accepted",
    );
    let bounce = &records[2];
    assert_eq!(bounce.content()["is_error"], true);
    assert!(
        bounce.content()["output"]
            .as_str()
            .expect("the bounce output is a string")
            .contains("reference no candidate"),
        "the bounce diagnostics teach the model what to fix",
    );

    teardown(ctx).await;
}
