//! The relation submission pipeline: the deterministic validators behind
//! `submit_result` for the relation loop.
//!
//! Schema parse first, then the checks, in order, never a model call.
//! Every rejection bounces with diagnostics held to the prompt bar so the
//! loop's model corrects its own output. Endpoint existence is rechecked at
//! the commit boundary, not here, so a claim that vanished between the
//! model's read and its submission drops the edge rather than failing the
//! stage; this pipeline validates only what the model controls: the shape,
//! the bounds, that no edge is a self-edge or a duplicate, and that every
//! endpoint is a claim the system actually offered as a citable reference.
//!
//! Provenance is checked against a structured set of trusted ids, not the
//! substring corpus: the batch's committed items and the existing claims
//! triage surfaced (seeded from the stage's loaded context), extended only
//! through grounded tool results harvested from the thread log at submission
//! time. A search result is grounded by construction; an id-addressed read
//! extends the set only when the id it was given was already citable, so the
//! set grows by walking out from grounded anchors. An id that appears only
//! inside externally-derived content (a candidate's body, an item's body)
//! cannot enter the set, whether cited directly or read back out of a read's
//! own echoed id field, so it cannot be laundered into a permanent
//! cross-project edge.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use serde::Deserialize;
use sqlx::PgConnection;
use tribal_agent_runtime::{
    RecordedToolCall, SeenCorpus, SubmissionOutcome, SubmissionPipeline, ToolResultContent,
    VerifierLaunch,
};
use tribal_db::{AgentThreadRecordRepository, PgAgentThreadRecordRepository};
use tribal_domain::{
    AgentThread, AgentThreadRecordKind, KnowledgeItemId, RelationKind, ToolFailure,
};

use crate::{
    parsing::{RELATION_JUSTIFICATION_MAX_CHARS, RelationSubmission, RelationSubmissionEdge},
    tools::{RelationToolGrounding, relation_tool_grounding},
};

/// The relation stage's submission pipeline. Cross-project by nature, so it
/// carries no project fence; provenance is the structured trusted-id set,
/// not the project a tool reached.
pub(crate) struct RelationSubmissionPipeline {
    /// The ids the opening offered up front: the batch's committed items and
    /// the existing claims triage surfaced. The model's tool results extend
    /// this at submission time. Seeded from the stage's loaded context rather
    /// than re-queried, because a stage thread carries no job id to query by.
    citable_seed: HashSet<KnowledgeItemId>,
}

impl RelationSubmissionPipeline {
    /// Creates the pipeline seeded with the ids the opening offered as
    /// citable references.
    pub(crate) fn new(citable_seed: HashSet<KnowledgeItemId>) -> Self {
        Self { citable_seed }
    }
}

#[async_trait]
impl SubmissionPipeline for RelationSubmissionPipeline {
    async fn evaluate(
        &self,
        conn: &mut PgConnection,
        thread: &AgentThread,
        _corpus: &SeenCorpus,
        arguments: &serde_json::Value,
    ) -> Result<SubmissionOutcome, ToolFailure> {
        // 1. Schema parse is free, and the diagnostics teach the shape.
        let submission: RelationSubmission = match serde_json::from_value(arguments.clone()) {
            Ok(submission) => submission,
            Err(source) => {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "the submission does not match its schema: {source}; check the \
                         submit_result schema and resubmit"
                    ),
                });
            }
        };

        // An empty edge list is a valid, complete answer with nothing to
        // ground, so it accepts without reconstructing the citable set.
        if submission.relations.is_empty() {
            return accept(&submission);
        }

        // Membership is checked against the structured set of ids the system
        // offered as citable: the seed from the opening, extended with the
        // ids the model's own tool results surfaced. A foreign id seen only
        // inside externally-derived content is absent here and cannot be
        // laundered into an edge.
        let mut citable = self.citable_seed.clone();
        harvest_tool_result_ids(conn, thread, &mut citable).await?;
        if let Err(diagnostics) = check_relation_edges(&submission.relations, &citable) {
            return Ok(SubmissionOutcome::Bounced { diagnostics });
        }

        accept(&submission)
    }

    async fn verifier_launch(
        &self,
        _conn: &mut PgConnection,
        _thread: &AgentThread,
        _payload: &serde_json::Value,
    ) -> Result<Option<VerifierLaunch>, ToolFailure> {
        // The relation loop ships without a verifier in this release; an
        // accepted submission commits on the validators alone.
        Ok(None)
    }
}

/// Serialises an accepted submission into the payload the loop commits.
fn accept(submission: &RelationSubmission) -> Result<SubmissionOutcome, ToolFailure> {
    let payload = serde_json::to_value(submission).map_err(|source| ToolFailure::System {
        context: format!("serialising the accepted submission: {source}"),
    })?;
    Ok(SubmissionOutcome::Accepted { payload })
}

/// The relation stage's deterministic per-edge validators, factored from the
/// provenance reconstruction so they test without a database. Returns the
/// first edge's rejection diagnostic, or `Ok(())` when every edge is
/// well-shaped, grounded in the citable set, not a self-edge, and not a
/// repeat of an edge already listed in the submission.
fn check_relation_edges(
    edges: &[RelationSubmissionEdge],
    citable: &HashSet<KnowledgeItemId>,
) -> Result<(), String> {
    let mut seen: HashSet<(KnowledgeItemId, KnowledgeItemId, RelationKind)> = HashSet::new();
    for edge in edges {
        // The justification bound: curated reasoning, never a transcript.
        if let Some(justification) = &edge.justification
            && justification.chars().count() > RELATION_JUSTIFICATION_MAX_CHARS
        {
            return Err(format!(
                "an edge justification is too long ({} characters against a bound of \
                 {RELATION_JUSTIFICATION_MAX_CHARS}); keep it to the reasoning for the edge",
                justification.chars().count(),
            ));
        }

        // The id shape: an unparseable endpoint is a copy fault the model
        // can fix by copying the id exactly.
        let source_id = parse_endpoint(&edge.source_id)?;
        let target_id = parse_endpoint(&edge.target_id)?;

        // Provenance: both endpoints must be ids the system offered as
        // citable references, not ids found only inside externally-derived
        // content.
        let uncitable: Vec<&str> = [(&edge.source_id, source_id), (&edge.target_id, target_id)]
            .into_iter()
            .filter(|(_, id)| !citable.contains(id))
            .map(|(raw, _)| raw.as_str())
            .collect();
        if !uncitable.is_empty() {
            return Err(format!(
                "these ids were not offered as citable references: {}; cite only the batch items, \
                 the claims triage surfaced, and ids a search or read returned, copied exactly",
                uncitable.join(", "),
            ));
        }

        // No self-edge: an edge connects two different claims.
        if source_id == target_id {
            return Err(format!(
                "the edge from {source_id} to itself is not a relationship; an edge connects two \
                 different claims",
            ));
        }

        // No duplicate triple: each directed, typed edge is listed at most
        // once.
        let relation_type: RelationKind = edge.relation_type.into();
        if !seen.insert((source_id, target_id, relation_type)) {
            return Err(format!(
                "the edge from {source_id} to {target_id} ({}) is listed more than once; list \
                 each directed, typed edge at most once",
                relation_type.as_str(),
            ));
        }
    }
    Ok(())
}

/// Parses a model-supplied endpoint id, rendering the copy-exactly rule into
/// the diagnostic on failure.
fn parse_endpoint(raw: &str) -> Result<KnowledgeItemId, String> {
    raw.parse::<KnowledgeItemId>().map_err(|_| {
        format!(
            "'{raw}' is not a knowledge item id; copy ids exactly as rendered (the '{}_' prefix \
             followed by a UUID)",
            KnowledgeItemId::PREFIX,
        )
    })
}

/// Extends `ids` with the knowledge item ids the model's grounded tool
/// results surfaced under an `item_id` or `*_item_id` field. Read by thread
/// id, which a stage thread always carries (unlike its job id), in log order
/// so trust walks out from grounded anchors: a search result is grounded by
/// construction, an id-addressed read only when the id it was given is
/// already citable. An id that appears only inside content is never citable,
/// and reading it back out of a read's echoed id field does not ground it.
async fn harvest_tool_result_ids(
    conn: &mut PgConnection,
    thread: &AgentThread,
    ids: &mut HashSet<KnowledgeItemId>,
) -> Result<(), ToolFailure> {
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(conn, thread.id())
        .await
        .map_err(|source| ToolFailure::System {
            context: format!("reading thread records for relation provenance: {source}"),
        })?;

    // The call behind each result, keyed by its id: a result grounds
    // provenance only through the call that produced it.
    let calls: HashMap<String, RecordedToolCall> = records
        .iter()
        .filter(|record| record.kind() == AgentThreadRecordKind::AssistantMessage)
        .filter_map(|record| {
            serde_json::from_value::<AssistantToolCalls>(record.content().clone()).ok()
        })
        .flat_map(|content| content.tool_calls)
        .map(|call| (call.id.clone(), call))
        .collect();

    for record in &records {
        if record.kind() != AgentThreadRecordKind::ToolResult {
            continue;
        }
        let Ok(content) = serde_json::from_value::<ToolResultContent>(record.content().clone())
        else {
            continue;
        };
        if content.is_error {
            continue;
        }
        let Some(call) = record.tool_call_id().and_then(|id| calls.get(id)) else {
            continue;
        };
        if !call_grounds_provenance(call, ids) {
            continue;
        }
        // A grounded result is whole, valid JSON (§10.1); a parse failure is
        // an internal inconsistency, not a skip.
        let value =
            serde_json::from_str::<serde_json::Value>(&content.output).map_err(|source| {
                ToolFailure::System {
                    context: format!("a grounded relation tool result is not valid JSON: {source}"),
                }
            })?;
        collect_item_id_fields(&value, ids);
    }
    Ok(())
}

/// The assistant message's tool calls, the only field the harvest reads off a
/// committed record.
#[derive(Deserialize)]
struct AssistantToolCalls {
    #[serde(default)]
    tool_calls: Vec<RecordedToolCall>,
}

/// Whether a call's result may extend the citable set, given the set as it
/// stands. A search is grounded by construction; an id-addressed read grounds
/// its result only when the id it was given is already citable, so an id seen
/// only inside content cannot bootstrap trust by being read back out of the
/// read's own echoed id field.
fn call_grounds_provenance(call: &RecordedToolCall, citable: &HashSet<KnowledgeItemId>) -> bool {
    match relation_tool_grounding(call.name.as_str()) {
        RelationToolGrounding::Search => true,
        RelationToolGrounding::IdAddressedRead => call
            .arguments
            .get("item_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|raw| raw.parse::<KnowledgeItemId>().ok())
            .is_some_and(|id| citable.contains(&id)),
        RelationToolGrounding::NotProvenance => false,
    }
}

/// Walks a tool result's JSON, collecting every well-formed knowledge item id
/// that appears under an `item_id` or `*_item_id` key. Ids embedded in
/// free-text fields (an item's content) are deliberately not collected: only
/// the structured id fields the tools emit are trusted provenance.
fn collect_item_id_fields(value: &serde_json::Value, ids: &mut HashSet<KnowledgeItemId>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                if (key == "item_id" || key.ends_with("_item_id"))
                    && let Some(raw) = child.as_str()
                    && let Ok(id) = raw.parse::<KnowledgeItemId>()
                {
                    ids.insert(id);
                }
                collect_item_id_fields(child, ids);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                collect_item_id_fields(child, ids);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use tribal_db::{
        AgentThreadRepository, DrivingTaskRef, JobRepository, NewAgentThread, NewAgentThreadRecord,
        PgAgentThreadRepository, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
        PgTaskRepository, PrincipalRepository, ProjectRepository, TaskRepository,
    };
    use tribal_domain::{AGENT_THREAD_FORMAT_VERSION, GitRemote, TaskType};
    use tribal_test_utils::{
        a_new_job, a_new_principal, a_new_project, a_new_prompt_version, a_new_system_fingerprint,
        a_new_task, an_agent_definition, an_agent_thread, insert_prompt_version, serial_lock,
        test_context, upsert_system_fingerprint,
    };

    use super::*;
    use crate::parsing::IngestionRelationKind;

    fn ki(suffix: &str) -> KnowledgeItemId {
        format!("ki_{suffix}0000-0000-0000-0000-000000000000")
            .parse()
            .expect("a valid knowledge item id")
    }

    fn edge(source: &KnowledgeItemId, target: &KnowledgeItemId) -> RelationSubmissionEdge {
        RelationSubmissionEdge {
            source_id: source.to_string(),
            target_id: target.to_string(),
            relation_type: IngestionRelationKind::Supports,
            justification: None,
        }
    }

    // -- check_relation_edges: the pure per-edge validators --

    #[test]
    fn test_check_accepts_a_grounded_distinct_edge() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        let citable = HashSet::from([a, b]);
        assert!(check_relation_edges(&[edge(&a, &b)], &citable).is_ok());
    }

    #[test]
    fn test_check_bounces_an_uncitable_endpoint() {
        let a = ki("aaaa");
        let foreign = ki("ffff");
        // Only `a` was offered as citable; `foreign` was never surfaced as a
        // trusted id, so the edge to it cannot be laundered through.
        let citable = HashSet::from([a]);
        let diagnostics = check_relation_edges(&[edge(&a, &foreign)], &citable)
            .expect_err("an uncitable endpoint bounces");
        assert!(
            diagnostics.contains(&foreign.to_string())
                && diagnostics.contains("citable references"),
        );
    }

    #[test]
    fn test_check_bounces_an_unparseable_id_on_shape() {
        let b = ki("bbbb");
        let citable = HashSet::from([b]);
        let mut malformed = edge(&b, &b);
        malformed.source_id = "not-an-id".to_owned();
        let diagnostics =
            check_relation_edges(&[malformed], &citable).expect_err("an unparseable id bounces");
        assert!(diagnostics.contains("copy ids exactly"));
    }

    #[test]
    fn test_check_bounces_a_self_edge() {
        let a = ki("aaaa");
        let citable = HashSet::from([a]);
        let diagnostics =
            check_relation_edges(&[edge(&a, &a)], &citable).expect_err("a self-edge bounces");
        assert!(diagnostics.contains("two different claims"));
    }

    #[test]
    fn test_check_bounces_a_duplicate_triple() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        let citable = HashSet::from([a, b]);
        let diagnostics = check_relation_edges(&[edge(&a, &b), edge(&a, &b)], &citable)
            .expect_err("a repeated triple bounces");
        assert!(diagnostics.contains("more than once"));
    }

    #[test]
    fn test_check_accepts_inverse_edges() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        let citable = HashSet::from([a, b]);
        assert!(
            check_relation_edges(&[edge(&a, &b), edge(&b, &a)], &citable).is_ok(),
            "an inverse edge is a distinct triple",
        );
    }

    #[test]
    fn test_check_bounces_an_overlong_justification() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        let citable = HashSet::from([a, b]);
        let mut long = edge(&a, &b);
        long.justification = Some("x".repeat(RELATION_JUSTIFICATION_MAX_CHARS + 1));
        let diagnostics =
            check_relation_edges(&[long], &citable).expect_err("an overlong justification bounces");
        assert!(diagnostics.contains("too long"));
    }

    // -- collect_item_id_fields: the harvest's content immunity --

    #[test]
    fn test_collect_harvests_id_fields_not_ids_inside_content() {
        let surfaced = ki("dddd");
        let foreign = ki("eeee");
        // A search-result page shape: the trusted `item_id` field alongside a
        // free-text `content` field that happens to mention a foreign id.
        let page = serde_json::json!({
            "results": [{
                "item_id": surfaced.to_string(),
                "kind": "fact",
                "content": format!("a claim that also names {foreign}"),
                "tags": [],
                "similarity_score": 0.9,
            }],
            "next_cursor": null,
        });
        let mut ids = HashSet::new();
        collect_item_id_fields(&page, &mut ids);
        assert!(ids.contains(&surfaced), "the id field is harvested");
        assert!(
            !ids.contains(&foreign),
            "an id only inside content is not harvested",
        );
    }

    #[test]
    fn test_collect_harvests_relation_endpoint_id_fields() {
        let anchor = ki("aaaa");
        let from = ki("bbbb");
        let to = ki("cccc");
        // A neighbourhood-read shape: the endpoint ids ride `*_item_id` keys.
        let neighbourhood = serde_json::json!({
            "item_id": anchor.to_string(),
            "inbound_relations": [{ "relation": "supports", "from_item_id": from.to_string() }],
            "outbound_relations": [{ "relation": "supports", "to_item_id": to.to_string() }],
            "neighbours": [],
        });
        let mut ids = HashSet::new();
        collect_item_id_fields(&neighbourhood, &mut ids);
        assert_eq!(ids, HashSet::from([anchor, from, to]));
    }

    #[test]
    fn test_collect_ignores_an_unparseable_id_field() {
        let value = serde_json::json!({ "item_id": "not-an-id" });
        let mut ids = HashSet::new();
        collect_item_id_fields(&value, &mut ids);
        assert!(ids.is_empty());
    }

    // -- call_grounds_provenance: which results extend the citable set --

    fn a_call(name: &str, item_id: Option<&str>) -> RecordedToolCall {
        RecordedToolCall {
            id: "call_x".to_owned(),
            name: name.to_owned(),
            arguments: match item_id {
                Some(id) => serde_json::json!({ "item_id": id }),
                None => serde_json::json!({}),
            },
        }
    }

    #[test]
    fn test_a_search_result_is_grounded_by_construction() {
        // A search ranks real items, so its result grounds whatever the
        // citable set holds.
        assert!(call_grounds_provenance(
            &a_call("search_related_items", None),
            &HashSet::new(),
        ));
    }

    #[test]
    fn test_a_read_grounds_only_a_citable_id() {
        let anchor = ki("aaaa");
        let foreign = ki("ffff");
        let citable = HashSet::from([anchor]);
        // The anchor is citable, so reading it grounds; the foreign id, seen
        // only inside content, does not become citable by being read.
        assert!(call_grounds_provenance(
            &a_call("read_knowledge_item", Some(&anchor.to_string())),
            &citable,
        ));
        assert!(!call_grounds_provenance(
            &a_call("read_knowledge_item", Some(&foreign.to_string())),
            &citable,
        ));
        assert!(call_grounds_provenance(
            &a_call("read_item_neighbourhood", Some(&anchor.to_string())),
            &citable,
        ));
    }

    #[test]
    fn test_a_read_of_a_malformed_or_absent_id_does_not_ground() {
        let citable = HashSet::from([ki("aaaa")]);
        assert!(!call_grounds_provenance(
            &a_call("read_knowledge_item", Some("not-an-id")),
            &citable,
        ));
        assert!(!call_grounds_provenance(
            &a_call("read_knowledge_item", None),
            &citable,
        ));
    }

    #[test]
    fn test_an_unknown_tool_grounds_nothing() {
        assert!(!call_grounds_provenance(
            &a_call("submit_result", None),
            &HashSet::from([ki("aaaa")]),
        ));
    }

    // -- evaluate: schema, empty, and seed-only membership (no records) --

    #[tokio::test]
    async fn test_a_malformed_submission_bounces_with_the_schema_diagnostics() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = RelationSubmissionPipeline::new(HashSet::new())
            .evaluate(
                &mut txn,
                &thread,
                &SeenCorpus::default(),
                &serde_json::json!({ "edges": [] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("schema")
        ));
    }

    #[tokio::test]
    async fn test_an_empty_submission_is_accepted() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = RelationSubmissionPipeline::new(HashSet::new())
            .evaluate(
                &mut txn,
                &thread,
                &SeenCorpus::default(),
                &serde_json::json!({ "relations": [] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn test_an_id_absent_from_the_seed_and_tool_results_bounces() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        // An unpersisted thread has no tool-result records, so the citable
        // set is the seed alone: a foreign id the model only saw inside
        // candidate content (never the seed, never a tool result) bounces.
        let thread = an_agent_thread().build();
        let committed = ki("aaaa");
        let foreign = ki("ffff");

        let outcome = RelationSubmissionPipeline::new(HashSet::from([committed]))
            .evaluate(
                &mut txn,
                &thread,
                &SeenCorpus::default(),
                &serde_json::json!({ "relations": [edge(&committed, &foreign)] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains(&foreign.to_string())
                    && diagnostics.contains("citable references")
        ));
    }

    // -- evaluate: ids harvested from the model's own tool results --

    /// Persists a relation stage thread with no job id, as the runtime
    /// creates one: the harvest must rely on the thread id alone.
    async fn insert_relation_thread(conn: &mut sqlx::PgConnection) -> AgentThread {
        let principal = PgPrincipalRepository
            .insert(
                conn,
                &a_new_principal()
                    .principal_key("user:relation-provenance".to_owned())
                    .build(),
            )
            .await
            .expect("principal");
        let project = PgProjectRepository
            .insert(
                conn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts("github.com", "test/relation", None))
                    .build(),
            )
            .await
            .expect("project");

        let pv_id = insert_prompt_version(conn, &a_new_prompt_version().build()).await;
        let fingerprint =
            upsert_system_fingerprint(conn, &a_new_system_fingerprint().build()).await;
        let job = PgJobRepository
            .insert(
                conn,
                &a_new_job()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .extraction_system_prompt_version_id(pv_id)
                    .extraction_user_prompt_version_id(pv_id)
                    .triage_system_prompt_version_id(pv_id)
                    .triage_user_prompt_version_id(pv_id)
                    .relation_system_prompt_version_id(pv_id)
                    .relation_user_prompt_version_id(pv_id)
                    .system_fingerprint_hash(fingerprint)
                    .build(),
            )
            .await
            .expect("seed job");
        let task = PgTaskRepository
            .insert(
                conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Relation)
                    .build(),
            )
            .await
            .expect("seed relation task");
        let binding = tribal_agent_runtime::resolve_binding(
            conn,
            &an_agent_definition()
                .pipeline_stage(TaskType::Relation)
                .build(),
        )
        .await
        .expect("resolve binding");
        PgAgentThreadRepository
            .insert(
                conn,
                &NewAgentThread::builder()
                    .pipeline_stage(TaskType::Relation)
                    .binding_version_id(binding.id())
                    .driving_task(DrivingTaskRef::Stage(task.id()))
                    .principal_id(principal.id())
                    .format_version(AGENT_THREAD_FORMAT_VERSION)
                    .build(),
            )
            .await
            .expect("insert thread")
    }

    /// Persists a relation thread whose log carries the search the model ran:
    /// a result page surfacing `searched` by its `item_id` field while naming
    /// `laundered` only inside the result's content.
    async fn seed_relation_thread(
        conn: &mut sqlx::PgConnection,
        searched: KnowledgeItemId,
        laundered: KnowledgeItemId,
    ) -> AgentThread {
        let thread = insert_relation_thread(conn).await;
        let page = serde_json::json!({
            "results": [{
                "item_id": searched.to_string(),
                "kind": "fact",
                "content": format!("a surfaced claim that also names {laundered}"),
                "tags": [],
                "similarity_score": 0.9,
            }],
            "next_cursor": null,
        });
        append_tool_call(
            conn,
            &thread,
            "search_related_items",
            serde_json::json!({}),
            &page,
        )
        .await;
        thread
    }

    /// Appends the assistant call/result pair the harvest reads: an assistant
    /// message naming `tool_name` with `arguments`, then a non-error result
    /// carrying the serialised `result`. The call id pairs the two.
    async fn append_tool_call(
        conn: &mut sqlx::PgConnection,
        thread: &AgentThread,
        tool_name: &str,
        arguments: serde_json::Value,
        result: &serde_json::Value,
    ) {
        let call_seq = PgAgentThreadRecordRepository
            .next_seq(conn, thread.id())
            .await
            .expect("call seq");
        let call_id = format!("call_{call_seq}");
        PgAgentThreadRecordRepository
            .append(
                conn,
                &NewAgentThreadRecord::builder()
                    .thread_id(thread.id())
                    .seq(call_seq)
                    .kind(AgentThreadRecordKind::AssistantMessage)
                    .content(serde_json::json!({
                        "text": "",
                        "tool_calls": [{ "id": call_id, "name": tool_name, "arguments": arguments }],
                    }))
                    .build(),
            )
            .await
            .expect("append tool call");
        let result_seq = PgAgentThreadRecordRepository
            .next_seq(conn, thread.id())
            .await
            .expect("result seq");
        PgAgentThreadRecordRepository
            .append(
                conn,
                &NewAgentThreadRecord::builder()
                    .thread_id(thread.id())
                    .seq(result_seq)
                    .kind(AgentThreadRecordKind::ToolResult)
                    .requesting_seq(Some(call_seq))
                    .tool_call_id(Some(call_id))
                    .content(serde_json::json!({
                        "output": serde_json::to_string(result).expect("serialise result"),
                        "is_error": false,
                    }))
                    .build(),
            )
            .await
            .expect("append tool result");
    }

    #[tokio::test]
    async fn test_evaluate_accepts_an_id_a_tool_result_surfaced() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let committed = ki("aaaa");
        let searched = ki("5555");
        let laundered = ki("ffff");
        let thread = seed_relation_thread(&mut txn, searched, laundered).await;

        // The committed id is in the seed; the searched id was surfaced by a
        // tool result's `item_id` field and harvested. Both are citable.
        let outcome = RelationSubmissionPipeline::new(HashSet::from([committed]))
            .evaluate(
                &mut txn,
                &thread,
                &SeenCorpus::default(),
                &serde_json::json!({ "relations": [edge(&committed, &searched)] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn test_evaluate_bounces_a_foreign_id_laundered_through_content() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let committed = ki("aaaa");
        let searched = ki("5555");
        let laundered = ki("ffff");
        let thread = seed_relation_thread(&mut txn, searched, laundered).await;

        // The foreign id appears only inside the search result's content,
        // never as an `item_id` field, so it is not harvested: an edge to it
        // bounces rather than minting a permanent cross-project edge.
        let outcome = RelationSubmissionPipeline::new(HashSet::from([committed]))
            .evaluate(
                &mut txn,
                &thread,
                &SeenCorpus::default(),
                &serde_json::json!({ "relations": [edge(&committed, &laundered)] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains(&laundered.to_string())
                    && diagnostics.contains("citable references")
        ));
    }

    #[tokio::test]
    async fn test_evaluate_does_not_launder_an_id_read_from_content() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let committed = ki("aaaa");
        let foreign = ki("ffff");
        let thread = insert_relation_thread(&mut txn).await;

        // The model reads a foreign id it saw only inside content. The read
        // echoes that id in its `item_id` field, but the read was grounded by
        // neither the seed nor a prior search, so the echo cannot make it
        // citable and an edge to it bounces.
        let item = serde_json::json!({
            "item_id": foreign.to_string(),
            "kind": "fact",
            "content": "a foreign claim",
            "tags": [],
            "created_at": "2026-01-01T00:00:00Z",
        });
        append_tool_call(
            &mut txn,
            &thread,
            "read_knowledge_item",
            serde_json::json!({ "item_id": foreign.to_string() }),
            &item,
        )
        .await;

        let outcome = RelationSubmissionPipeline::new(HashSet::from([committed]))
            .evaluate(
                &mut txn,
                &thread,
                &SeenCorpus::default(),
                &serde_json::json!({ "relations": [edge(&committed, &foreign)] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains(&foreign.to_string())
                    && diagnostics.contains("citable references")
        ));
    }

    #[tokio::test]
    async fn test_evaluate_grounds_a_neighbour_read_from_a_citable_anchor() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let committed = ki("aaaa");
        let anchor = ki("5555");
        let neighbour = ki("6666");
        let thread = insert_relation_thread(&mut txn).await;

        // A search grounds the anchor; reading the anchor's neighbourhood is
        // then grounded too, so a neighbour it surfaces becomes citable and
        // can anchor an edge. Trust walks out from the grounded anchor.
        let page = serde_json::json!({
            "results": [{
                "item_id": anchor.to_string(),
                "kind": "fact",
                "content": "a surfaced claim",
                "tags": [],
                "similarity_score": 0.9,
            }],
            "next_cursor": null,
        });
        append_tool_call(
            &mut txn,
            &thread,
            "search_related_items",
            serde_json::json!({}),
            &page,
        )
        .await;
        let neighbourhood = serde_json::json!({
            "item_id": anchor.to_string(),
            "inbound_relations": [],
            "outbound_relations": [{ "relation": "supports", "to_item_id": neighbour.to_string() }],
            "neighbours": [{
                "item_id": neighbour.to_string(),
                "kind": "fact",
                "content": "a neighbouring claim",
                "tags": [],
                "created_at": "2026-01-01T00:00:00Z",
            }],
        });
        append_tool_call(
            &mut txn,
            &thread,
            "read_item_neighbourhood",
            serde_json::json!({ "item_id": anchor.to_string() }),
            &neighbourhood,
        )
        .await;

        let outcome = RelationSubmissionPipeline::new(HashSet::from([committed]))
            .evaluate(
                &mut txn,
                &thread,
                &SeenCorpus::default(),
                &serde_json::json!({ "relations": [edge(&committed, &neighbour)] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));
    }
}
