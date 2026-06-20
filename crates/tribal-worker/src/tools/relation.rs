//! The relation stage's cross-project tools.
//!
//! Relation is the one stage that reasons across the project fence: it
//! connects a batch's freshly committed items to the wider graph, so its
//! search reaches every project and its reads resolve any item the search
//! surfaced. The neighbourhood read stays inside the batch's project set by
//! default, the conservative reach the design pins pending an eval that
//! shows traversing further helps.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use tribal_agent_runtime::{RecordedToolCall, StageTool, ToolOutcome};
use tribal_db::{
    DbError, KnowledgeItemRepository, PgKnowledgeItemRepository, PgRelationRepository,
    RelationRepository, SemanticSearchParams,
};
use tribal_domain::{
    EmbeddingProfile, EmbeddingPurpose, KnowledgeItem, KnowledgeItemId, KnowledgeKind, ProjectId,
    RecoverableToolFailure, ToolDescriptor, ToolExecutionMode, ToolFailure, ToolSafetyTier,
};
use tribal_inference::{
    EmbeddingRequest, EmbeddingTarget, InferenceGateway, PermitWait, UsageAttribution,
};

use super::{db_failure, paged_search_within_bound, parse_arguments, serialise_outcome};
use crate::parsing::relation_submission_schema;

// ---------------------------------------------------------------------------
// Shared views
// ---------------------------------------------------------------------------

/// One knowledge item as the relation reads render it. No project id: the
/// model relates claims by content, and exposing internal project ids would
/// leak structure without informing the decision.
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

/// Parses a model-supplied item id, rendering the copy-exactly rule into
/// the diagnostic on failure.
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

/// The miss rendering shared by the id-addressed reads.
fn missing_item(tool: &str, raw_id: &str) -> ToolFailure {
    ToolFailure::Recoverable(RecoverableToolFailure::EmptyResult {
        tool: tool.to_owned(),
        detail: format!("no item with id {raw_id} exists"),
    })
}

// ---------------------------------------------------------------------------
// search_related_items
// ---------------------------------------------------------------------------

const SEARCH_NAME: &str = "search_related_items";
const SEARCH_RESPONSE_SIZE_BOUND: u32 = 16_384;
const SEARCH_EXPECTED: &str = r#"{"query": "<text describing the claims to find>", "cursor": "<next-page cursor, optional>"}"#;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArguments {
    /// The text the search embeds and ranks items against.
    query: String,
    /// The opaque page cursor a prior result returned; absent for the
    /// first page.
    #[serde(default)]
    cursor: Option<String>,
}

/// One similar item as the relation search renders it.
#[derive(Debug, Serialize)]
struct SearchResultView {
    item_id: String,
    kind: KnowledgeKind,
    content: String,
    tags: Vec<String>,
    similarity_score: f64,
}

/// One page of cross-project search results.
#[derive(Debug, Serialize)]
struct SearchResults {
    results: Vec<SearchResultView>,
    next_cursor: Option<String>,
}

/// What `prepare` hands `execute`: the query embedding and the page cursor.
#[derive(Debug, Serialize, Deserialize)]
struct PreparedSearch {
    embedding: Vec<f32>,
    cursor: Option<String>,
}

/// Cross-project semantic search over the whole knowledge graph.
///
/// Embeds the model's query text and ranks every project's items against
/// it, so the relation agent can find existing claims anywhere to connect
/// the batch to. `prepare` embeds with no transaction held; `execute` runs
/// the unfenced, cursor-paginated search.
pub(crate) struct SearchRelatedItemsTool {
    descriptor: ToolDescriptor,
    profile: EmbeddingProfile,
    gateway: Arc<InferenceGateway>,
    attribution: UsageAttribution,
    search_limit: u32,
    deadline: tokio::time::Instant,
}

impl SearchRelatedItemsTool {
    /// The tool's declared contract: the binding-hash input the lockstep
    /// definition derivation reads without constructing the tool.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(SEARCH_NAME.to_owned())
            .description(
                "Search what is already known, across every project, for claims similar to \
                 your query text. Returns items with their ids, kinds, content, tags, and \
                 similarity scores, most similar first, plus a next-page cursor. Use it to find \
                 existing claims the batch's items might relate to. Only items returned here, or \
                 shown in your batch, can be cited in a relationship."
                    .to_owned(),
            )
            .input_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Text describing the claims to find."
                    },
                    "cursor": {
                        "type": "string",
                        "description": "The next-page cursor from a prior result; omit for the \
                                        first page."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            }))
            .response_size_bound(SEARCH_RESPONSE_SIZE_BOUND)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build()
    }

    /// Creates the cross-project search tool for one stage execution.
    pub(crate) fn new(
        profile: EmbeddingProfile,
        gateway: Arc<InferenceGateway>,
        attribution: UsageAttribution,
        search_limit: u32,
        deadline: tokio::time::Instant,
    ) -> Self {
        let descriptor = Self::describe();
        Self {
            descriptor,
            profile,
            gateway,
            attribution,
            search_limit,
            deadline,
        }
    }
}

#[async_trait]
impl StageTool for SearchRelatedItemsTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn prepare(
        &self,
        arguments: &serde_json::Value,
    ) -> Result<serde_json::Value, ToolFailure> {
        let args: SearchArguments = parse_arguments(SEARCH_NAME, arguments, SEARCH_EXPECTED)?;
        let request = EmbeddingRequest {
            input: args.query,
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
                context: format!("relation search query embedding call: {source}"),
            })?;
        serde_json::to_value(PreparedSearch {
            embedding: response.vector,
            cursor: args.cursor,
        })
        .map_err(|source| ToolFailure::System {
            context: format!("serialising the prepared relation search: {source}"),
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
                context: "relation search executed without its prepared embedding".to_owned(),
            })?;
        let params = SemanticSearchParams::builder()
            .query_embedding(prepared.embedding)
            .embedding_profile_id(self.profile.id())
            .dimensions(self.profile.dimensions())
            // No project fence: relation reaches the whole graph.
            .project_id(None)
            .limit(self.search_limit)
            .cursor(prepared.cursor)
            .build();
        paged_search_within_bound(
            conn,
            SEARCH_NAME,
            "serialising relation search results",
            params,
            self.descriptor.response_size_bound,
            |response, rendering| SearchResults {
                results: response
                    .results
                    .iter()
                    .map(|result| SearchResultView {
                        item_id: result.item.id().to_string(),
                        kind: result.item.kind(),
                        content: rendering.apply(result.item.content()),
                        tags: rendering.tags(result.item.tags()),
                        similarity_score: result.similarity,
                    })
                    .collect(),
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

/// Reads one knowledge item in full, in any project. The read resolves any
/// id; grounding a read result so it can back an edge is the submission
/// pipeline's provenance rule, not this tool's, so the reader stays plain.
pub(crate) struct ReadKnowledgeItemTool {
    descriptor: ToolDescriptor,
}

impl ReadKnowledgeItemTool {
    /// The tool's declared contract: the binding-hash input.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(READ_ITEM_NAME.to_owned())
            .description(
                "Read a knowledge item by id, in any project, returning its kind, content, tags, \
                 and creation time. Use it to inspect a claim in full before deciding how the \
                 batch relates to it."
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

    pub(crate) fn new() -> Self {
        Self {
            descriptor: Self::describe(),
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
        let id = parse_item_id(READ_ITEM_NAME, &args.item_id)?;
        let item = match PgKnowledgeItemRepository.find_by_id(conn, id).await {
            Ok(item) => item,
            Err(DbError::NotFound { .. }) => {
                return Err(missing_item(READ_ITEM_NAME, &args.item_id));
            }
            Err(source) => return Err(db_failure("reading a knowledge item", &source)),
        };
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

/// Reads an item's committed relations and the neighbouring items they
/// connect to within the batch's project set.
///
/// The anchor may be any item a cross-project search surfaced; the fence
/// applies to the neighbours, not the anchor. Neighbours outside the
/// batch's projects are dropped, so the read shows how a claim sits in the
/// graph among the batch's own projects rather than traversing the whole
/// graph. The fenced set is the projects the batch's items belong to.
pub(crate) struct ReadItemNeighbourhoodTool {
    descriptor: ToolDescriptor,
    batch_projects: HashSet<ProjectId>,
}

impl ReadItemNeighbourhoodTool {
    /// The tool's declared contract: the binding-hash input.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(NEIGHBOURHOOD_NAME.to_owned())
            .description(
                "Read a knowledge item's committed relations and the neighbouring items they \
                 connect within the batch's projects. Use it to see how a claim already relates \
                 to what is already known before adding a relationship to it."
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

    pub(crate) fn new(batch_projects: HashSet<ProjectId>) -> Self {
        Self {
            descriptor: Self::describe(),
            batch_projects,
        }
    }
}

// ---------------------------------------------------------------------------
// Submission provenance grounding
// ---------------------------------------------------------------------------

/// How a relation tool's result contributes to submission provenance. A
/// search ranks real items, so its results are grounded by construction; an
/// id-addressed read echoes the id it was given, so it grounds its result
/// only when that id was already citable; any other tool grounds nothing.
pub(crate) enum RelationToolGrounding {
    Search,
    IdAddressedRead,
    NotProvenance,
}

impl RelationToolGrounding {
    fn of(tool_name: &str) -> Self {
        match tool_name {
            SEARCH_NAME => Self::Search,
            READ_ITEM_NAME | NEIGHBOURHOOD_NAME => Self::IdAddressedRead,
            _ => Self::NotProvenance,
        }
    }

    /// Whether the call's result may extend the citable set, given the set as
    /// it stands. An id seen only inside content cannot bootstrap trust by
    /// being read back out of the read's own echoed id field.
    pub(crate) fn call_grounds(
        call: &RecordedToolCall,
        citable: &HashSet<KnowledgeItemId>,
    ) -> bool {
        match Self::of(&call.name) {
            Self::Search => true,
            Self::IdAddressedRead => call
                .arguments
                .get("item_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|raw| raw.parse::<KnowledgeItemId>().ok())
                .is_some_and(|id| citable.contains(&id)),
            Self::NotProvenance => false,
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
        let anchor_id = parse_item_id(NEIGHBOURHOOD_NAME, &args.item_id)?;
        let anchor = match PgKnowledgeItemRepository.find_by_id(conn, anchor_id).await {
            Ok(item) => item,
            Err(DbError::NotFound { .. }) => {
                return Err(missing_item(NEIGHBOURHOOD_NAME, &args.item_id));
            }
            Err(source) => return Err(db_failure("reading the anchor item", &source)),
        };

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
            .filter(|item| self.batch_projects.contains(&item.project_id()))
            .collect();
        let in_set: HashSet<KnowledgeItemId> = neighbours.iter().map(KnowledgeItem::id).collect();

        let view = NeighbourhoodView {
            item_id: anchor.id().to_string(),
            inbound_relations: inbound
                .iter()
                .filter(|relation| in_set.contains(&relation.source_id()))
                .map(|relation| InboundRelationView {
                    relation: relation.relation_type().as_str(),
                    from_item_id: relation.source_id().to_string(),
                    justification: relation.justification().map(str::to_owned),
                })
                .collect(),
            outbound_relations: outbound
                .iter()
                .filter(|relation| in_set.contains(&relation.target_id()))
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
// submit_result (the relation completion tool)
// ---------------------------------------------------------------------------

const SUBMIT_RESPONSE_SIZE_BOUND: u32 = 8_192;

/// The distinguished completion tool's contract for relation. Like triage's
/// submit, the loop dispatches it through the submission pipeline; this
/// descriptor shapes the relation submission the model returns and what the
/// binding hashes.
pub(crate) fn submit_relations_descriptor() -> ToolDescriptor {
    ToolDescriptor::builder()
        .name(tribal_agent_runtime::SUBMIT_RESULT_TOOL.to_owned())
        .description(
            "Submit the relationships for this batch. This is the only way to \
             complete the task: a list of directed relationships, each connecting two claims by \
             the exact item ids you were shown or that a tool returned. Submit an empty list when \
             nothing genuinely relates. A rejected submission comes back with diagnostics; \
             correct it and resubmit."
                .to_owned(),
        )
        .input_schema(relation_submission_schema())
        .response_size_bound(SUBMIT_RESPONSE_SIZE_BOUND)
        .safety_tier(ToolSafetyTier::InternalTransactional)
        .execution_mode(ToolExecutionMode::Immediate)
        .build()
}
