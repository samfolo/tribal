//! The triage stage's project-scoped tools.
//!
//! Dedup is project-local, so every read here is fenced to the project
//! captured at construction. A read of another project's item renders
//! exactly as a miss: scope is enforced without offering an existence
//! oracle over the rest of the store.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use tribal_agent_runtime::{StageTool, ToolOutcome};
use tribal_db::{
    DbError, KnowledgeItemRepository, PgKnowledgeItemRepository, PgRelationRepository,
    PgTagRegistryRepository, RelationRepository, SemanticSearchParams, TagRegistryRepository,
};
use tribal_domain::{
    EmbeddingProfile, EmbeddingPurpose, KnowledgeItem, KnowledgeItemId, KnowledgeKind, ProjectId,
    RecoverableToolFailure, ToolDescriptor, ToolExecutionMode, ToolFailure, ToolSafetyTier,
};
use tribal_inference::{
    EmbeddingRequest, EmbeddingTarget, InferenceGateway, PermitWait, UsageAttribution,
};

use super::{db_failure, paged_search_within_bound, parse_arguments, serialise_outcome};
use crate::{parsing::triage_submission_schema, prompt::LoopSimilarItemContext};

// ---------------------------------------------------------------------------
// Shared views
// ---------------------------------------------------------------------------

/// One knowledge item as the read tools render it.
#[derive(Debug, Serialize)]
struct ItemView {
    item_id: String,
    kind: KnowledgeKind,
    content: String,
    tags: Vec<String>,
    created_at: DateTime<Utc>,
}

impl From<&KnowledgeItem> for ItemView {
    fn from(item: &KnowledgeItem) -> Self {
        Self {
            item_id: item.id().to_string(),
            kind: item.kind(),
            content: item.content().to_owned(),
            tags: item.tags().to_vec(),
            created_at: item.created_at(),
        }
    }
}

/// Parses a model-supplied item id, rendering the copy-exactly rule
/// into the diagnostic on failure.
fn parse_item_id(tool: &str, raw: &str) -> Result<KnowledgeItemId, ToolFailure> {
    raw.parse().map_err(|_| {
        ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments {
            tool: tool.to_owned(),
            detail: format!(
                "'{raw}' is not a knowledge item id; copy ids exactly as rendered \
                 (the '{}_' prefix followed by a UUID)",
                KnowledgeItemId::PREFIX,
            ),
        })
    })
}

/// The miss rendering shared by the id-addressed reads: a foreign
/// project's item renders identically to one that does not exist.
fn missing_item(tool: &str, raw_id: &str) -> ToolFailure {
    ToolFailure::Recoverable(RecoverableToolFailure::EmptyResult {
        tool: tool.to_owned(),
        detail: format!("no item with id {raw_id} exists in this project"),
    })
}

/// Reads an item and applies the project fence, mapping a repository
/// miss and an out-of-project hit onto the same rendering.
async fn read_project_item(
    conn: &mut PgConnection,
    tool: &str,
    project_id: ProjectId,
    raw_id: &str,
) -> Result<KnowledgeItem, ToolFailure> {
    let id = parse_item_id(tool, raw_id)?;
    let item = match PgKnowledgeItemRepository.find_by_id(conn, id).await {
        Ok(item) => item,
        Err(DbError::NotFound { .. }) => return Err(missing_item(tool, raw_id)),
        Err(source) => return Err(db_failure("reading a knowledge item", &source)),
    };
    if item.project_id() != project_id {
        return Err(missing_item(tool, raw_id));
    }
    Ok(item)
}

// ---------------------------------------------------------------------------
// search_candidate_similar_items
// ---------------------------------------------------------------------------

const SEARCH_NAME: &str = "search_candidate_similar_items";
const SEARCH_RESPONSE_SIZE_BOUND: u32 = 16_384;
const SEARCH_EXPECTED: &str = r#"{"cursor": "<next-page cursor, or omitted for the first page>"}"#;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArguments {
    /// The opaque page cursor a prior result returned; absent for the
    /// first page.
    #[serde(default)]
    cursor: Option<String>,
}

/// One page of the candidate's similar items, with the page cursor and the
/// embedding profile the scores were computed under, so the model
/// paginates and the commit reconstructs scores from the durable record.
#[derive(Debug, Serialize)]
struct CandidateSearchResults {
    results: Vec<LoopSimilarItemContext>,
    embedding_profile_id: String,
    next_cursor: Option<String>,
}

/// What `prepare` hands `execute`: the candidate embedding (computed once)
/// and the page cursor the model supplied.
#[derive(Debug, Serialize, Deserialize)]
struct PreparedSearch {
    embedding: Vec<f32>,
    cursor: Option<String>,
}

/// Candidate-bound semantic search over the project's knowledge items.
///
/// Embeds the *fixed candidate* rather than arbitrary query text, so every
/// score is candidate-to-item similarity, the score the decision rows and
/// relation's similarity band read. The embedding is computed once and
/// reused across pages; the model walks the neighbourhood through the page
/// cursor. `prepare` embeds with no transaction held; `execute` runs the
/// project-scoped, cursor-paginated search.
pub(crate) struct SearchCandidateSimilarItemsTool {
    descriptor: ToolDescriptor,
    project_id: ProjectId,
    profile: EmbeddingProfile,
    gateway: Arc<InferenceGateway>,
    attribution: UsageAttribution,
    search_limit: u32,
    deadline: tokio::time::Instant,
    candidate_content: String,
    embedding: tokio::sync::OnceCell<Vec<f32>>,
}

impl SearchCandidateSimilarItemsTool {
    /// The tool's declared contract: the binding-hash input the
    /// lockstep definition derivation reads without constructing the
    /// tool.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(SEARCH_NAME.to_owned())
            .description(
                "Search what is already known in this project for items similar to the \
                 candidate under triage. Returns items with their ids, kinds, content, tags, and \
                 candidate-similarity scores, most similar first, plus a next-page cursor. \
                 Call it with no cursor for the first page, then pass the returned cursor to \
                 walk further. You must search before you can classify: only items returned \
                 here can be cited as duplicates or similar."
                    .to_owned(),
            )
            .input_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "cursor": {
                        "type": "string",
                        "description": "The next-page cursor from a prior result; omit for \
                                        the first page."
                    }
                },
                "additionalProperties": false
            }))
            .response_size_bound(SEARCH_RESPONSE_SIZE_BOUND)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build()
    }

    /// Creates the candidate-bound search tool for one stage execution: the
    /// candidate whose embedding scores every page, the project fence, the
    /// active embedding profile the search space is keyed to, and the
    /// attribution its embedding spend is metered under.
    pub(crate) fn new(
        candidate_content: String,
        project_id: ProjectId,
        profile: EmbeddingProfile,
        gateway: Arc<InferenceGateway>,
        attribution: UsageAttribution,
        search_limit: u32,
        deadline: tokio::time::Instant,
    ) -> Self {
        let descriptor = Self::describe();
        Self {
            descriptor,
            project_id,
            profile,
            gateway,
            attribution,
            search_limit,
            deadline,
            candidate_content,
            embedding: tokio::sync::OnceCell::new(),
        }
    }

    /// Embeds the candidate once, reusing the cached vector across pages.
    async fn candidate_embedding(&self) -> Result<&Vec<f32>, ToolFailure> {
        self.embedding
            .get_or_try_init(|| async {
                let request = EmbeddingRequest {
                    input: self.candidate_content.clone(),
                    purpose: EmbeddingPurpose::Query,
                };
                let remaining = self
                    .deadline
                    .saturating_duration_since(tokio::time::Instant::now());
                let response = self
                    .gateway
                    .embed(
                        &EmbeddingTarget::from(&self.profile),
                        request,
                        PermitWait::Bounded { limit: remaining },
                        &self.attribution,
                    )
                    .await
                    .map_err(|source| ToolFailure::System {
                        context: format!("candidate embedding call: {source}"),
                    })?;
                Ok(response.vector)
            })
            .await
    }
}

#[async_trait]
impl StageTool for SearchCandidateSimilarItemsTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn prepare(
        &self,
        arguments: &serde_json::Value,
    ) -> Result<serde_json::Value, ToolFailure> {
        let args: SearchArguments = parse_arguments(SEARCH_NAME, arguments, SEARCH_EXPECTED)?;
        let embedding = self.candidate_embedding().await?.clone();
        serde_json::to_value(PreparedSearch {
            embedding,
            cursor: args.cursor,
        })
        .map_err(|source| ToolFailure::System {
            context: format!("serialising the prepared candidate search: {source}"),
        })
    }

    async fn execute(
        &self,
        conn: &mut PgConnection,
        prepared: serde_json::Value,
        _arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        let prepared: PreparedSearch =
            serde_json::from_value(prepared).map_err(|_| ToolFailure::System {
                context: "candidate search executed without its prepared embedding".to_owned(),
            })?;
        let params = SemanticSearchParams::builder()
            .query_embedding(prepared.embedding)
            .embedding_profile_id(self.profile.id())
            .dimensions(self.profile.dimensions())
            .project_id(Some(self.project_id))
            .limit(self.search_limit)
            .cursor(prepared.cursor)
            .build();
        let profile_id = self.profile.id().to_string();
        paged_search_within_bound(
            conn,
            SEARCH_NAME,
            "serialising candidate similar-item search results",
            params,
            self.descriptor.response_size_bound,
            |response, rendering| CandidateSearchResults {
                results: response
                    .results
                    .iter()
                    .map(|result| {
                        let mut context = LoopSimilarItemContext::from(result);
                        context.content = rendering.apply(&context.content);
                        context.tags = rendering.tags(&context.tags);
                        context
                    })
                    .collect(),
                embedding_profile_id: profile_id.clone(),
                next_cursor: response.next_cursor.clone(),
            },
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// read_knowledge_item
// ---------------------------------------------------------------------------

const READ_ITEM_NAME: &str = "read_knowledge_item";
const READ_ITEM_RESPONSE_SIZE_BOUND: u32 = 8_192;
const READ_ITEM_EXPECTED: &str = r#"{"item_id": "<a rendered knowledge item id>"}"#;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadItemArguments {
    item_id: String,
}

/// Reads one of the project's knowledge items in full.
pub(crate) struct ReadKnowledgeItemTool {
    descriptor: ToolDescriptor,
    project_id: ProjectId,
}

impl ReadKnowledgeItemTool {
    /// The tool's declared contract: the binding-hash input the
    /// lockstep definition derivation reads without constructing the
    /// tool.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(READ_ITEM_NAME.to_owned())
            .description(
                "Read one of this project's knowledge items by id, returning its kind, \
                 content, tags, and creation time. Use it to inspect an item in full before \
                 judging how the candidate relates to it."
                    .to_owned(),
            )
            .input_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "item_id": {
                        "type": "string",
                        "description": "A knowledge item id, copied exactly as rendered."
                    }
                },
                "required": ["item_id"],
                "additionalProperties": false
            }))
            .response_size_bound(READ_ITEM_RESPONSE_SIZE_BOUND)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build()
    }

    /// Creates the read tool fenced to one project.
    pub(crate) fn new(project_id: ProjectId) -> Self {
        let descriptor = Self::describe();
        Self {
            descriptor,
            project_id,
        }
    }
}

#[async_trait]
impl StageTool for ReadKnowledgeItemTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn execute(
        &self,
        conn: &mut PgConnection,
        _prepared: serde_json::Value,
        arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        let args: ReadItemArguments =
            parse_arguments(READ_ITEM_NAME, arguments, READ_ITEM_EXPECTED)?;
        let item = read_project_item(conn, READ_ITEM_NAME, self.project_id, &args.item_id).await?;
        serialise_outcome("serialising a knowledge item read", &ItemView::from(&item))
    }
}

// ---------------------------------------------------------------------------
// read_item_neighbourhood
// ---------------------------------------------------------------------------

const NEIGHBOURHOOD_NAME: &str = "read_item_neighbourhood";
const NEIGHBOURHOOD_RESPONSE_SIZE_BOUND: u32 = 16_384;
const NEIGHBOURHOOD_EXPECTED: &str = r#"{"item_id": "<a rendered knowledge item id>"}"#;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NeighbourhoodArguments {
    item_id: String,
}

#[derive(Debug, Serialize)]
struct InboundRelationView {
    relation: &'static str,
    from_item_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    justification: Option<String>,
}

#[derive(Debug, Serialize)]
struct OutboundRelationView {
    relation: &'static str,
    to_item_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    justification: Option<String>,
}

#[derive(Debug, Serialize)]
struct NeighbourhoodView {
    item_id: String,
    inbound_relations: Vec<InboundRelationView>,
    outbound_relations: Vec<OutboundRelationView>,
    neighbours: Vec<ItemView>,
}

/// Reads an item's committed relations and their in-project neighbours.
///
/// Relations whose far endpoint lies outside the project are omitted
/// with their endpoints: triage reasons within its own project, and the
/// cross-project graph belongs to the relation stage's tools.
pub(crate) struct ReadItemNeighbourhoodTool {
    descriptor: ToolDescriptor,
    project_id: ProjectId,
}

impl ReadItemNeighbourhoodTool {
    /// The tool's declared contract: the binding-hash input the
    /// lockstep definition derivation reads without constructing the
    /// tool.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(NEIGHBOURHOOD_NAME.to_owned())
            .description(
                "Read a knowledge item's committed relations and the neighbouring items they \
                 connect within this project, to see how an item already relates to what is \
                 already known before deciding the candidate's relationship to it. Optional and \
                 rarely needed: reach for it only when those existing relations would change your \
                 classification."
                    .to_owned(),
            )
            .input_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "item_id": {
                        "type": "string",
                        "description": "A knowledge item id, copied exactly as rendered."
                    }
                },
                "required": ["item_id"],
                "additionalProperties": false
            }))
            .response_size_bound(NEIGHBOURHOOD_RESPONSE_SIZE_BOUND)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build()
    }

    /// Creates the neighbourhood tool fenced to one project.
    pub(crate) fn new(project_id: ProjectId) -> Self {
        let descriptor = Self::describe();
        Self {
            descriptor,
            project_id,
        }
    }
}

#[async_trait]
impl StageTool for ReadItemNeighbourhoodTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn execute(
        &self,
        conn: &mut PgConnection,
        _prepared: serde_json::Value,
        arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        let args: NeighbourhoodArguments =
            parse_arguments(NEIGHBOURHOOD_NAME, arguments, NEIGHBOURHOOD_EXPECTED)?;
        let anchor =
            read_project_item(conn, NEIGHBOURHOOD_NAME, self.project_id, &args.item_id).await?;

        let inbound = PgRelationRepository
            .find_inbound(conn, anchor.id(), None)
            .await
            .map_err(|source| db_failure("reading inbound relations", &source))?;
        let outbound = PgRelationRepository
            .find_outbound(conn, anchor.id(), None)
            .await
            .map_err(|source| db_failure("reading outbound relations", &source))?;

        let neighbour_ids: Vec<KnowledgeItemId> = inbound
            .iter()
            .map(tribal_domain::KnowledgeItemRelation::source_id)
            .chain(
                outbound
                    .iter()
                    .map(tribal_domain::KnowledgeItemRelation::target_id),
            )
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let neighbours: Vec<KnowledgeItem> = PgKnowledgeItemRepository
            .find_by_ids(conn, &neighbour_ids)
            .await
            .map_err(|source| db_failure("reading neighbouring items", &source))?
            .into_iter()
            .filter(|item| item.project_id() == self.project_id)
            .collect();
        let in_project: HashSet<KnowledgeItemId> =
            neighbours.iter().map(KnowledgeItem::id).collect();

        let view = NeighbourhoodView {
            item_id: anchor.id().to_string(),
            inbound_relations: inbound
                .iter()
                .filter(|relation| in_project.contains(&relation.source_id()))
                .map(|relation| InboundRelationView {
                    relation: relation.relation_type().as_str(),
                    from_item_id: relation.source_id().to_string(),
                    justification: relation.justification().map(str::to_owned),
                })
                .collect(),
            outbound_relations: outbound
                .iter()
                .filter(|relation| in_project.contains(&relation.target_id()))
                .map(|relation| OutboundRelationView {
                    relation: relation.relation_type().as_str(),
                    to_item_id: relation.target_id().to_string(),
                    justification: relation.justification().map(str::to_owned),
                })
                .collect(),
            neighbours: neighbours.iter().map(ItemView::from).collect(),
        };
        serialise_outcome("serialising an item neighbourhood", &view)
    }
}

// ---------------------------------------------------------------------------
// list_tag_registry
// ---------------------------------------------------------------------------

const TAG_REGISTRY_NAME: &str = "list_tag_registry";
const TAG_REGISTRY_RESPONSE_SIZE_BOUND: u32 = 8_192;
const TAG_REGISTRY_EXPECTED: &str = "no arguments";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TagRegistryArguments {}

#[derive(Debug, Serialize)]
struct TagView {
    tag: String,
    usage_count: i32,
}

#[derive(Debug, Serialize)]
struct TagRegistryView {
    tags: Vec<TagView>,
}

/// Lists the tag registry.
pub(crate) struct ListTagRegistryTool {
    descriptor: ToolDescriptor,
}

impl ListTagRegistryTool {
    /// The tool's declared contract: the binding-hash input the
    /// lockstep definition derivation reads without constructing the
    /// tool.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(TAG_REGISTRY_NAME.to_owned())
            .description(
                "List every tag in the registry with its usage count. Consult it before \
                 suggesting tags, so the existing vocabulary is reused instead of fragmented."
                    .to_owned(),
            )
            .input_schema(serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }))
            .response_size_bound(TAG_REGISTRY_RESPONSE_SIZE_BOUND)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build()
    }

    /// Creates the tag registry listing tool. The registry is shared
    /// vocabulary across projects, so no fence applies.
    pub(crate) fn new() -> Self {
        let descriptor = Self::describe();
        Self { descriptor }
    }
}

#[async_trait]
impl StageTool for ListTagRegistryTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn execute(
        &self,
        conn: &mut PgConnection,
        _prepared: serde_json::Value,
        arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        let _: TagRegistryArguments =
            parse_arguments(TAG_REGISTRY_NAME, arguments, TAG_REGISTRY_EXPECTED)?;
        let entries = PgTagRegistryRepository
            .find_all(conn)
            .await
            .map_err(|source| db_failure("listing the tag registry", &source))?;
        let view = TagRegistryView {
            tags: entries
                .iter()
                .map(|entry| TagView {
                    tag: entry.tag().to_owned(),
                    usage_count: entry.usage_count(),
                })
                .collect(),
        };
        serialise_outcome("serialising the tag registry", &view)
    }
}

// ---------------------------------------------------------------------------
// submit_result
// ---------------------------------------------------------------------------

const SUBMIT_RESPONSE_SIZE_BOUND: u32 = 8_192;

/// The distinguished completion tool's contract. Deliberately not a
/// [`StageTool`]: the loop dispatches `submit_result` through the
/// submission pipeline, and this descriptor only shapes what the model
/// sees and what the binding hashes.
pub(crate) fn submit_result_descriptor() -> ToolDescriptor {
    ToolDescriptor::builder()
        .name(tribal_agent_runtime::SUBMIT_RESULT_TOOL.to_owned())
        .description(
            "Submit your final triage decision. This is the only way to \
             complete the task: the decision, an assessment of each \
             existing claim you examined (referenced by the exact item ids \
             you were shown), and optional short notes for the downstream \
             relation stage. A rejected submission comes back with \
             diagnostics; correct it and resubmit."
                .to_owned(),
        )
        .input_schema(triage_submission_schema())
        .response_size_bound(SUBMIT_RESPONSE_SIZE_BOUND)
        .safety_tier(ToolSafetyTier::InternalTransactional)
        .execution_mode(ToolExecutionMode::Immediate)
        .build()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tribal_db::{
        JobStateOverride, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
        PrincipalRepository, ProjectRepository,
    };
    use tribal_domain::{
        GitRemote, JobOutcome, JobStatus, PrincipalId, ProviderKind, RelationBatchId, RelationKind,
        UsageOwner,
    };
    use tribal_inference::{
        InjectedEmbedding, InjectedProviders, NoopLedgerSink, ProviderKey, ProviderLimits,
        ProviderRegistry, RequestClass,
    };
    use tribal_test_utils::{
        MockEmbeddingProvider, MockInferenceProvider, TestDb, a_new_job, a_new_knowledge_item,
        a_new_principal, a_new_project, a_new_prompt_version, a_new_system_fingerprint,
        an_embedding_profile, an_embedding_response, ensure_genesis_profile,
        insert_committed_relation, insert_embedding_for_profile, insert_prompt_version,
        upsert_system_fingerprint,
    };

    use super::*;

    const EMBEDDING_MODEL: &str = "text-embedding-test";
    const DIMENSIONS: u32 = 768;

    fn basis_vector(dominant: usize) -> Vec<f32> {
        let mut vector = vec![0.0f32; DIMENSIONS as usize];
        vector[dominant] = 1.0;
        vector
    }

    fn unowned_attribution() -> UsageAttribution {
        UsageAttribution {
            owner: UsageOwner::Unowned,
            principal_id: None,
            system_prompt_version_id: None,
            user_prompt_version_id: None,
            trace_id: None,
        }
    }

    fn a_deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(30)
    }

    /// A gateway whose embedding provider is the given mock, bound to
    /// the profile, with no ledger writes.
    fn embed_gateway(
        profile_id: tribal_domain::EmbeddingProfileId,
        embedding: Arc<MockEmbeddingProvider>,
    ) -> Arc<InferenceGateway> {
        let limits = || ProviderLimits {
            max_in_flight: 10,
            request_timeout: Duration::from_secs(30),
        };
        let inference_key =
            ProviderKey::new("mock", "http://localhost:9999", RequestClass::Inference)
                .expect("valid inference key");
        // The gateway resolves the embedding semaphore from the profile's
        // recorded endpoint, which the genesis profile pins to the Ollama
        // default.
        let embedding_key = ProviderKey::new(
            ProviderKind::Ollama.to_string(),
            ProviderKind::DEFAULT_OLLAMA_BASE_URL,
            RequestClass::Embedding,
        )
        .expect("valid embedding key");
        let registry = ProviderRegistry::new(vec![
            (inference_key.clone(), limits()),
            (embedding_key, limits()),
        ])
        .expect("valid registry");
        Arc::new(InferenceGateway::with_providers(
            InjectedProviders::uniform(
                registry,
                Arc::new(MockInferenceProvider::builder().build()),
                inference_key,
                vec![InjectedEmbedding {
                    profile_id,
                    provider: embedding,
                }],
                Arc::new(NoopLedgerSink),
            ),
        ))
    }

    async fn seed_actors(
        conn: &mut sqlx::PgConnection,
        suffix: &str,
    ) -> (PrincipalId, tribal_domain::ProjectId) {
        let principal = PgPrincipalRepository
            .insert(
                conn,
                &a_new_principal()
                    .principal_key(format!("user:tools-{suffix}"))
                    .build(),
            )
            .await
            .expect("insert principal");
        let project = PgProjectRepository
            .insert_git(
                conn,
                &a_new_project()
                    .remote(GitRemote::from_parts(
                        "github.com",
                        &format!("test/tools-{suffix}"),
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("insert project");
        (principal.id(), project.id())
    }

    async fn seed_item(
        conn: &mut sqlx::PgConnection,
        principal_id: PrincipalId,
        project_id: tribal_domain::ProjectId,
        content: &str,
    ) -> KnowledgeItem {
        PgKnowledgeItemRepository
            .insert(
                conn,
                &a_new_knowledge_item()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .content(content.to_owned())
                    .build(),
            )
            .await
            .expect("insert item")
    }

    fn parsed(outcome: &ToolOutcome) -> serde_json::Value {
        serde_json::from_str(&outcome.content).expect("tool outcomes are JSON")
    }

    #[tokio::test]
    async fn test_candidate_search_embeds_the_candidate_then_searches_only_the_project() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let (principal_id, project_a) = seed_actors(&mut txn, "search-a").await;
        let (_, project_b) = seed_actors(&mut txn, "search-b").await;
        let profile = ensure_genesis_profile(&mut txn, EMBEDDING_MODEL, DIMENSIONS).await;

        // Identical embeddings either side of the fence: only the scope
        // can separate them.
        let item_a = seed_item(&mut txn, principal_id, project_a, "claim in project A").await;
        insert_embedding_for_profile(
            &mut txn,
            item_a.id(),
            profile.id(),
            EMBEDDING_MODEL,
            basis_vector(0),
        )
        .await;
        let item_b = seed_item(&mut txn, principal_id, project_b, "claim in project B").await;
        insert_embedding_for_profile(
            &mut txn,
            item_b.id(),
            profile.id(),
            EMBEDDING_MODEL,
            basis_vector(0),
        )
        .await;

        let embedding = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(basis_vector(0)), None)
                .build(),
        );
        let tool = SearchCandidateSimilarItemsTool::new(
            "does this claim already exist".to_owned(),
            project_a,
            profile.clone(),
            embed_gateway(profile.id(), Arc::clone(&embedding)),
            unowned_attribution(),
            10,
            a_deadline(),
        );

        // The first page carries no cursor; prepare embeds the candidate.
        let arguments = serde_json::json!({});
        let prepared = tool.prepare(&arguments).await.expect("prepare embeds");
        let history = embedding.embedding_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].input, "does this claim already exist");
        assert_eq!(history[0].purpose, EmbeddingPurpose::Query);

        let outcome = tool
            .execute(&mut txn, prepared, &arguments)
            .await
            .expect("execute searches");
        let view = parsed(&outcome);
        let results = view["results"].as_array().expect("results array");
        assert_eq!(
            results.len(),
            1,
            "the foreign project's item must not match"
        );
        assert_eq!(results[0]["item_id"], item_a.id().to_string());
        assert_eq!(results[0]["content"], "claim in project A");
        assert_eq!(
            view["embedding_profile_id"],
            profile.id().to_string(),
            "the profile marks the result so the commit can reconstruct its scores",
        );
    }

    #[tokio::test]
    async fn test_candidate_search_shrinks_an_oversized_page_to_fit_its_bound() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let (principal_id, project) = seed_actors(&mut txn, "search-oversized").await;
        let profile = ensure_genesis_profile(&mut txn, EMBEDDING_MODEL, DIMENSIONS).await;

        // Several large items all match the query, so a full page overflows
        // the size bound: the search must return fewer rows whole rather than
        // a byte-trimmed, unparseable page.
        let big = "x".repeat(5_000);
        for i in 0..6 {
            let item = seed_item(&mut txn, principal_id, project, &format!("{big} {i}")).await;
            insert_embedding_for_profile(
                &mut txn,
                item.id(),
                profile.id(),
                EMBEDDING_MODEL,
                basis_vector(0),
            )
            .await;
        }

        let embedding = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(basis_vector(0)), None)
                .build(),
        );
        let tool = SearchCandidateSimilarItemsTool::new(
            "does this claim already exist".to_owned(),
            project,
            profile.clone(),
            embed_gateway(profile.id(), Arc::clone(&embedding)),
            unowned_attribution(),
            10,
            a_deadline(),
        );

        let arguments = serde_json::json!({});
        let prepared = tool.prepare(&arguments).await.expect("prepare embeds");
        let outcome = tool
            .execute(&mut txn, prepared, &arguments)
            .await
            .expect("execute searches");

        assert!(
            outcome.content.len() <= SEARCH_RESPONSE_SIZE_BOUND as usize,
            "the page fits the bound: {} > {SEARCH_RESPONSE_SIZE_BOUND}",
            outcome.content.len(),
        );
        // A complete page parses; the trimmed bug would have produced invalid
        // JSON here, losing every score the model was shown.
        let view = parsed(&outcome);
        let results = view["results"].as_array().expect("results array");
        assert!(
            !results.is_empty() && results.len() < 6,
            "the page shrank to fewer whole rows, got {}",
            results.len(),
        );
        assert!(
            !view["next_cursor"].is_null(),
            "the rows that did not fit are reachable by cursor",
        );
    }

    #[tokio::test]
    async fn test_candidate_search_elides_a_single_item_larger_than_its_bound() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let (principal_id, project) = seed_actors(&mut txn, "search-elide").await;
        let profile = ensure_genesis_profile(&mut txn, EMBEDDING_MODEL, DIMENSIONS).await;

        // A single item whose content alone exceeds the bound: paging cannot
        // shrink below one row, so the search must elide its content to a
        // marker rather than return an oversized page the loop would flag an
        // error and skip, stranding the candidate with nothing to cite.
        let huge = "x".repeat(SEARCH_RESPONSE_SIZE_BOUND as usize + 1_000);
        let item = seed_item(&mut txn, principal_id, project, &huge).await;
        insert_embedding_for_profile(
            &mut txn,
            item.id(),
            profile.id(),
            EMBEDDING_MODEL,
            basis_vector(0),
        )
        .await;

        let embedding = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(basis_vector(0)), None)
                .build(),
        );
        let tool = SearchCandidateSimilarItemsTool::new(
            "does this claim already exist".to_owned(),
            project,
            profile.clone(),
            embed_gateway(profile.id(), Arc::clone(&embedding)),
            unowned_attribution(),
            10,
            a_deadline(),
        );

        let arguments = serde_json::json!({});
        let prepared = tool.prepare(&arguments).await.expect("prepare embeds");
        let outcome = tool
            .execute(&mut txn, prepared, &arguments)
            .await
            .expect("execute searches");

        // The elided page fits the bound and parses, so the loop records it as
        // a usable, non-error result rather than a trimmed one.
        assert!(
            outcome.content.len() <= SEARCH_RESPONSE_SIZE_BOUND as usize,
            "the elided page fits the bound: {}",
            outcome.content.len(),
        );
        let view = parsed(&outcome);
        let results = view["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1, "the single item is still returned");

        // Provenance survives the eliding: the id, score, and profile let the
        // model cite the item and fetch its full content.
        assert_eq!(results[0]["item_id"], item.id().to_string());
        assert!(results[0]["similarity_score"].is_number());
        assert_eq!(view["embedding_profile_id"], profile.id().to_string());
        let content = results[0]["content"]
            .as_str()
            .expect("the content is a string");
        assert!(
            content.contains("read_knowledge_item"),
            "the over-long content is elided to a marker: {content}",
        );
        assert!(
            !content.contains(huge.as_str()),
            "the full content is not inlined",
        );
    }

    #[tokio::test]
    async fn test_candidate_search_elides_an_item_with_oversized_tags() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let (principal_id, project) = seed_actors(&mut txn, "search-elide-tags").await;
        let profile = ensure_genesis_profile(&mut txn, EMBEDDING_MODEL, DIMENSIONS).await;

        // A single item whose content fits but whose tags alone push the page
        // past the bound: eliding must drop the tags too, or the "protected"
        // page still overflows and falls to the error path that strands the
        // candidate with nothing to cite.
        let many_tags: Vec<String> = (0..4_000).map(|i| format!("tag-{i:08}")).collect();
        let item = PgKnowledgeItemRepository
            .insert(
                &mut txn,
                &a_new_knowledge_item()
                    .project_id(project)
                    .principal_id(principal_id)
                    .content("a short claim".to_owned())
                    .tags(many_tags)
                    .build(),
            )
            .await
            .expect("insert item");
        insert_embedding_for_profile(
            &mut txn,
            item.id(),
            profile.id(),
            EMBEDDING_MODEL,
            basis_vector(0),
        )
        .await;

        let embedding = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(basis_vector(0)), None)
                .build(),
        );
        let tool = SearchCandidateSimilarItemsTool::new(
            "does this claim already exist".to_owned(),
            project,
            profile.clone(),
            embed_gateway(profile.id(), Arc::clone(&embedding)),
            unowned_attribution(),
            10,
            a_deadline(),
        );

        let arguments = serde_json::json!({});
        let prepared = tool.prepare(&arguments).await.expect("prepare embeds");
        let outcome = tool
            .execute(&mut txn, prepared, &arguments)
            .await
            .expect("execute searches");

        assert!(
            outcome.content.len() <= SEARCH_RESPONSE_SIZE_BOUND as usize,
            "the elided page fits the bound even with oversized tags: {}",
            outcome.content.len(),
        );
        let view = parsed(&outcome);
        let results = view["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1, "the single item is still returned");
        assert_eq!(results[0]["item_id"], item.id().to_string());
        let tags = results[0]["tags"].as_array().expect("tags array");
        assert!(tags.is_empty(), "the oversized tags are dropped on elision");
    }

    #[tokio::test]
    async fn test_candidate_search_maps_a_stale_cursor_to_a_recoverable_failure() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let profile = an_embedding_profile().build();
        let embedding = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(basis_vector(0)), None)
                .build(),
        );
        let tool = SearchCandidateSimilarItemsTool::new(
            "a candidate".to_owned(),
            tribal_domain::ProjectId::new(),
            profile.clone(),
            embed_gateway(profile.id(), Arc::clone(&embedding)),
            unowned_attribution(),
            10,
            a_deadline(),
        );

        // A malformed page cursor is the model's own input, so it returns
        // recoverable rather than failing the stage as a system fault.
        let arguments = serde_json::json!({"cursor": "not-a-valid-cursor"});
        let prepared = tool.prepare(&arguments).await.expect("prepare embeds");
        let err = tool
            .execute(&mut txn, prepared, &arguments)
            .await
            .expect_err("a malformed cursor is a recoverable input fault");
        assert!(matches!(
            err,
            ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments { ref tool, .. })
                if tool == SEARCH_NAME
        ));
    }

    #[tokio::test]
    async fn test_candidate_search_executed_without_its_prepared_embedding_is_a_system_failure() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let profile = an_embedding_profile().build();
        let embedding = Arc::new(MockEmbeddingProvider::builder().build());
        let tool = SearchCandidateSimilarItemsTool::new(
            "a candidate".to_owned(),
            tribal_domain::ProjectId::new(),
            profile,
            embed_gateway(tribal_domain::EmbeddingProfileId::new(), embedding),
            unowned_attribution(),
            10,
            a_deadline(),
        );

        let err = tool
            .execute(&mut txn, serde_json::Value::Null, &serde_json::json!({}))
            .await
            .expect_err("a skipped prepare phase must not reach the search");
        assert!(matches!(err, ToolFailure::System { .. }));
    }

    #[tokio::test]
    async fn test_candidate_search_embeds_once_across_pages_and_rejects_unknown_arguments() {
        let profile = an_embedding_profile().build();
        let embedding = Arc::new(
            MockEmbeddingProvider::builder()
                .on_embed(an_embedding_response(basis_vector(0)), None)
                .build(),
        );
        let tool = SearchCandidateSimilarItemsTool::new(
            "a candidate".to_owned(),
            tribal_domain::ProjectId::new(),
            profile.clone(),
            embed_gateway(profile.id(), Arc::clone(&embedding)),
            unowned_attribution(),
            10,
            a_deadline(),
        );

        // The first page (no cursor) and a later page (with one) both
        // prepare, but the candidate is embedded once and cached.
        tool.prepare(&serde_json::json!({}))
            .await
            .expect("the first page prepares");
        tool.prepare(&serde_json::json!({"cursor": "next-page"}))
            .await
            .expect("a later page prepares");
        assert_eq!(
            embedding.embedding_history().len(),
            1,
            "the candidate is embedded once across pages",
        );

        // An unknown argument is an argument fault, caught before any
        // provider call.
        let err = tool
            .prepare(&serde_json::json!({"page": 2}))
            .await
            .expect_err("an unknown argument must be rejected");
        assert!(matches!(
            err,
            ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments { ref tool, .. })
                if tool == SEARCH_NAME
        ));
    }

    #[tokio::test]
    async fn test_read_knowledge_item_renders_the_item_and_hides_foreign_projects() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let (principal_id, project_a) = seed_actors(&mut txn, "read-a").await;
        let (_, project_b) = seed_actors(&mut txn, "read-b").await;
        let item_a = seed_item(&mut txn, principal_id, project_a, "the project's own claim").await;
        let item_b = seed_item(&mut txn, principal_id, project_b, "a foreign claim").await;

        let tool = ReadKnowledgeItemTool::new(project_a);

        let outcome = tool
            .execute(
                &mut txn,
                serde_json::Value::Null,
                &serde_json::json!({"item_id": item_a.id().to_string()}),
            )
            .await
            .expect("an in-project read succeeds");
        let view = parsed(&outcome);
        assert_eq!(view["item_id"], item_a.id().to_string());
        assert_eq!(view["content"], "the project's own claim");
        assert!(view["kind"].is_string());
        assert!(view["tags"].is_array());

        // A foreign item and a non-existent one render identically, so
        // the tool offers no existence oracle over other projects.
        for raw_id in [item_b.id().to_string(), KnowledgeItemId::new().to_string()] {
            let err = tool
                .execute(
                    &mut txn,
                    serde_json::Value::Null,
                    &serde_json::json!({"item_id": raw_id}),
                )
                .await
                .expect_err("out-of-project reads must miss");
            assert!(matches!(
                err,
                ToolFailure::Recoverable(RecoverableToolFailure::EmptyResult { .. })
            ));
        }

        let err = tool
            .execute(
                &mut txn,
                serde_json::Value::Null,
                &serde_json::json!({"item_id": "not-an-id"}),
            )
            .await
            .expect_err("a malformed id is an argument fault");
        assert!(matches!(
            err,
            ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments { .. })
        ));
    }

    #[tokio::test]
    async fn test_neighbourhood_omits_relations_crossing_the_project_fence() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let (principal_id, project_a) = seed_actors(&mut txn, "hood-a").await;
        let (_, project_b) = seed_actors(&mut txn, "hood-b").await;

        let anchor = seed_item(&mut txn, principal_id, project_a, "the anchor claim").await;
        let inbound_neighbour =
            seed_item(&mut txn, principal_id, project_a, "supports the anchor").await;
        let outbound_neighbour =
            seed_item(&mut txn, principal_id, project_a, "anchored context").await;
        let foreign = seed_item(&mut txn, principal_id, project_b, "foreign claim").await;

        // Relations only count once their batch is committed through a job.
        let batch_id = RelationBatchId::new();
        let pv_id = insert_prompt_version(&mut txn, &a_new_prompt_version().build()).await;
        let fingerprint_hash =
            upsert_system_fingerprint(&mut txn, &a_new_system_fingerprint().build()).await;
        PgJobRepository
            .insert_for_test(
                &mut txn,
                &a_new_job()
                    .project_id(project_a)
                    .principal_id(principal_id)
                    .extraction_system_prompt_version_id(pv_id)
                    .extraction_user_prompt_version_id(pv_id)
                    .triage_system_prompt_version_id(pv_id)
                    .triage_user_prompt_version_id(pv_id)
                    .relation_system_prompt_version_id(pv_id)
                    .relation_user_prompt_version_id(pv_id)
                    .system_fingerprint_hash(fingerprint_hash)
                    .build(),
                &JobStateOverride::builder()
                    .status(JobStatus::Completed)
                    .outcome(Some(JobOutcome::Success))
                    .committed_batch_id(Some(batch_id))
                    .build(),
            )
            .await
            .expect("insert committed job");

        insert_committed_relation(
            &mut txn,
            batch_id,
            inbound_neighbour.id(),
            anchor.id(),
            RelationKind::Supports,
            principal_id,
        )
        .await;
        insert_committed_relation(
            &mut txn,
            batch_id,
            anchor.id(),
            outbound_neighbour.id(),
            RelationKind::DerivedFrom,
            principal_id,
        )
        .await;
        insert_committed_relation(
            &mut txn,
            batch_id,
            foreign.id(),
            anchor.id(),
            RelationKind::Supports,
            principal_id,
        )
        .await;

        let tool = ReadItemNeighbourhoodTool::new(project_a);
        let outcome = tool
            .execute(
                &mut txn,
                serde_json::Value::Null,
                &serde_json::json!({"item_id": anchor.id().to_string()}),
            )
            .await
            .expect("neighbourhood read succeeds");
        let view = parsed(&outcome);

        let inbound = view["inbound_relations"].as_array().expect("inbound array");
        assert_eq!(
            inbound.len(),
            1,
            "the cross-project inbound edge must be omitted"
        );
        assert_eq!(
            inbound[0]["from_item_id"],
            inbound_neighbour.id().to_string()
        );
        assert_eq!(inbound[0]["relation"], "supports");

        let outbound = view["outbound_relations"]
            .as_array()
            .expect("outbound array");
        assert_eq!(outbound.len(), 1);
        assert_eq!(
            outbound[0]["to_item_id"],
            outbound_neighbour.id().to_string()
        );

        assert!(
            !outcome.content.contains(&foreign.id().to_string()),
            "no foreign id may leak through any part of the rendering",
        );
        let neighbours = view["neighbours"].as_array().expect("neighbours array");
        assert_eq!(neighbours.len(), 2);
    }

    #[tokio::test]
    async fn test_list_tag_registry_renders_the_repository_entries() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");

        PgTagRegistryRepository
            .upsert(&mut txn, "error-handling")
            .await
            .expect("upsert tag");
        PgTagRegistryRepository
            .upsert(&mut txn, "testing")
            .await
            .expect("upsert tag");
        let entries = PgTagRegistryRepository
            .find_all(&mut txn)
            .await
            .expect("list entries");

        let tool = ListTagRegistryTool::new();
        let outcome = tool
            .execute(&mut txn, serde_json::Value::Null, &serde_json::json!({}))
            .await
            .expect("listing succeeds");
        let view = parsed(&outcome);
        let tags = view["tags"].as_array().expect("tags array");

        // The rendering mirrors the repository's entries one for one.
        assert_eq!(tags.len(), entries.len());
        for entry in &entries {
            assert!(
                tags.iter()
                    .any(|tag| tag["tag"] == entry.tag()
                        && tag["usage_count"] == entry.usage_count()),
                "entry '{}' must render with its usage count",
                entry.tag(),
            );
        }
    }
}
