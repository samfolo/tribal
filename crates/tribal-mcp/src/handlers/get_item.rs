//! Handler for `tribal_get_item` — batch item lookup by ID.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tracing::Instrument;
use tribal_db::DbError;
use tribal_domain::{
    KnowledgeItem, KnowledgeItemId, McpErrorCode, PrincipalId, Reference, Standing, span_attrs,
};

use super::common::acquire_connection;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{
        McpGetItemEntry, McpGetItemRequest, McpGetItemResponse, McpKnowledgeItem, McpReference,
        McpStanding,
    },
    server_handler::{ConnectionRepositories, TribalServerHandler},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_ITEM_IDS: usize = 20;

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Resolved domain parameters for the get-item operation.
struct GetItemParams {
    item_ids: Vec<KnowledgeItemId>,
    include_standing: bool,
    include_references: bool,
}

/// A single found item with resolved enrichment data.
#[derive(Debug)]
struct GetItemResultEntry {
    item: KnowledgeItem,
    principal_key: String,
    standing: Option<Standing>,
    references: Option<Vec<Reference>>,
}

/// Domain-level result from [`execute_get_item`].
#[derive(Debug)]
struct GetItemResult {
    found: HashMap<KnowledgeItemId, GetItemResultEntry>,
    not_found_ids: Vec<String>,
}

/// Service-boundary error for [`execute_get_item`].
#[derive(Debug, thiserror::Error)]
enum GetItemError {
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for GetItemError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

impl TribalServerHandler {
    /// Handles the `tribal_get_item` tool call.
    pub(crate) async fn handle_get_item(
        &self,
        params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let span = tracing::info_span!(
            parent: None,
            "tribal.get_item",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name(),
            { span_attrs::PROJECT_ID } = tracing::field::Empty,
        );
        self.apply_get_item(params).instrument(span).await
    }

    /// Core logic for `tribal_get_item`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates parameters, then delegates to
    /// [`execute_get_item`] for all domain logic. Domain errors are
    /// returned as error `CallToolResult` values via `IntoMcpError` /
    /// `IntoCallToolResult`. Only protocol-level errors (malformed JSON)
    /// return `Err(McpError)`.
    async fn apply_get_item(&self, params: serde_json::Value) -> Result<CallToolResult, McpError> {
        if let Some(project) = &self.session.read().await.project {
            tracing::Span::current()
                .record(span_attrs::PROJECT_ID, tracing::field::display(&project.id));
        }

        let request: McpGetItemRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        // -- Validate item_ids bounds ------------------------------------------

        if request.item_ids.is_empty() {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: "item_ids must contain at least 1 ID".into(),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        let count = request.item_ids.len();
        if count > MAX_ITEM_IDS {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: format!("item_ids must contain at most {MAX_ITEM_IDS} IDs, got {count}"),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        // -- Parse and validate each ID prefix ---------------------------------

        let mut parsed_ids = Vec::with_capacity(count);
        for raw in &request.item_ids {
            match KnowledgeItemId::from_str(raw) {
                Ok(id) => parsed_ids.push(id),
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            }
        }

        let raw_ids = request.item_ids;

        // -- Acquire connection and execute ------------------------------------

        let mut conn = match acquire_connection(
            &self.state.pool_mcp,
            self.config.pool_name,
            &self.state.metrics,
        )
        .await
        {
            Ok(c) => c,
            Err(call_result) => return Ok(call_result),
        };

        let get_item_params = GetItemParams {
            item_ids: parsed_ids.clone(),
            include_standing: request.include_standing.unwrap_or(false),
            include_references: request.include_references.unwrap_or(false),
        };

        let result = match execute_get_item(&mut conn, &self.repositories, get_item_params).await {
            Ok(r) => r,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        // -- Build response ----------------------------------------------------

        // Duplicate raw IDs are naturally collapsed by the map — the last
        // insert wins, producing one entry per unique ID.
        let mut items = serde_json::Map::with_capacity(raw_ids.len());

        for (raw_id, ki_id) in raw_ids.iter().zip(&parsed_ids) {
            if let Some(entry) = result.found.get(ki_id) {
                let mcp_entry = McpGetItemEntry {
                    item: McpKnowledgeItem::from_item_with_principal_key(
                        &entry.item,
                        &entry.principal_key,
                    ),
                    standing: entry.standing.as_ref().map(McpStanding::from),
                    references: entry
                        .references
                        .as_ref()
                        .map(|refs| refs.iter().map(McpReference::from).collect()),
                };
                let value = serde_json::to_value(&mcp_entry)
                    .expect("McpGetItemEntry should always serialise");
                items.insert(raw_id.clone(), value);
            } else {
                items.insert(raw_id.clone(), serde_json::Value::Null);
            }
        }

        let response = McpGetItemResponse {
            items,
            not_found_ids: result.not_found_ids,
        };

        Ok(response.into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Service function
// ---------------------------------------------------------------------------

/// Executes the get-item operation against the knowledge store.
///
/// This is the single application boundary called by the handler adapter.
/// All inputs and outputs are domain types — no MCP types cross this
/// boundary.
async fn execute_get_item(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    params: GetItemParams,
) -> Result<GetItemResult, GetItemError> {
    // -- Deduplicate input IDs -------------------------------------------------

    let item_ids: Vec<KnowledgeItemId> = {
        let mut seen = HashSet::new();
        let mut ids = params.item_ids;
        ids.retain(|id| seen.insert(*id));
        ids
    };

    // -- Batch lookup items ----------------------------------------------------

    let found_items = repositories
        .knowledge_item
        .find_by_ids(conn, &item_ids)
        .await?;

    let found_map: HashMap<KnowledgeItemId, KnowledgeItem> = found_items
        .into_iter()
        .map(|item| (item.id(), item))
        .collect();

    // -- Partition into found / not-found --------------------------------------

    let not_found_ids: Vec<String> = item_ids
        .iter()
        .filter(|id| !found_map.contains_key(id))
        .map(ToString::to_string)
        .collect();

    let found_ki_ids: Vec<KnowledgeItemId> = found_map.keys().copied().collect();

    if found_ki_ids.is_empty() {
        return Ok(GetItemResult {
            found: HashMap::new(),
            not_found_ids,
        });
    }

    // -- Resolve principal keys ------------------------------------------------

    let unique_principal_ids: Vec<PrincipalId> = {
        let mut ids: Vec<PrincipalId> = found_map
            .values()
            .map(KnowledgeItem::principal_id)
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };

    let principals = repositories
        .principal
        .find_by_ids(conn, &unique_principal_ids)
        .await?;
    let principal_map: HashMap<PrincipalId, String> = principals
        .into_iter()
        .map(|p| (p.id(), p.principal_key().to_owned()))
        .collect();

    // -- Optional enrichment ---------------------------------------------------

    let standings_map: Option<HashMap<KnowledgeItemId, Standing>> = if params.include_standing {
        let computed = repositories.standing.compute(conn, &found_ki_ids).await?;
        Some(found_ki_ids.iter().copied().zip(computed).collect())
    } else {
        None
    };

    let references_map: Option<HashMap<KnowledgeItemId, Vec<Reference>>> =
        if params.include_references {
            let all_refs = repositories
                .reference
                .find_by_knowledge_item_ids(conn, &found_ki_ids)
                .await?;
            let mut map: HashMap<KnowledgeItemId, Vec<Reference>> = HashMap::new();
            for r in all_refs {
                map.entry(r.knowledge_item_id()).or_default().push(r);
            }
            Some(map)
        } else {
            None
        };

    // -- Build result entries --------------------------------------------------

    let found: HashMap<KnowledgeItemId, GetItemResultEntry> = found_map
        .into_iter()
        .map(|(ki_id, item)| {
            let principal_key = principal_map
                .get(&item.principal_id())
                .cloned()
                .unwrap_or_else(|| item.principal_id().to_string());

            let standing = standings_map.as_ref().and_then(|m| m.get(&ki_id).cloned());
            let references = references_map
                .as_ref()
                .map(|m| m.get(&ki_id).cloned().unwrap_or_default());

            (
                ki_id,
                GetItemResultEntry {
                    item,
                    principal_key,
                    standing,
                    references,
                },
            )
        })
        .collect();

    Ok(GetItemResult {
        found,
        not_found_ids,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::{ErrorCode, RawContent};
    use tribal_domain::ProjectId;
    use tribal_test_utils::{
        MockKnowledgeItemRepository, MockPrincipalRepository, MockReferenceRepository,
        MockStandingRepository, TestDb, a_knowledge_item, a_principal, a_reference, a_standing,
    };

    use super::*;
    use crate::test_utils::{
        NO_STRUCTURED_CONTENT, TestHandler, first_text_content, test_repositories,
    };

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn test_item(ki_id: KnowledgeItemId, prin_id: PrincipalId) -> KnowledgeItem {
        a_knowledge_item().id(ki_id).principal_id(prin_id).build()
    }

    fn test_principal(id: PrincipalId, key: &str) -> tribal_domain::Principal {
        a_principal().id(id).principal_key(key.to_owned()).build()
    }

    fn default_params(item_ids: Vec<KnowledgeItemId>) -> GetItemParams {
        GetItemParams {
            item_ids,
            include_standing: false,
            include_references: false,
        }
    }

    fn repos_for_get_item(
        items: Vec<KnowledgeItem>,
        principals: Vec<tribal_domain::Principal>,
    ) -> ConnectionRepositories {
        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_ids(items, None)
            .build();

        let prin_mock = MockPrincipalRepository::builder()
            .on_find_by_ids(principals, None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.principal = Arc::new(prin_mock);
        repos
    }

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: GetItemParams,
    ) -> Result<GetItemResult, GetItemError> {
        let ctx = TestDb::new().await;
        let mut tx = ctx.begin().await.expect("begin");
        execute_get_item(&mut tx, repos, params).await
    }

    // -- Service: happy path -----------------------------------------------

    #[tokio::test]
    async fn test_single_item_found() {
        let ki_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id, prin_id);
        let principal = test_principal(prin_id, "user:alice");

        let repos = repos_for_get_item(vec![item], vec![principal]);
        let result = call_execute(&repos, default_params(vec![ki_id]))
            .await
            .expect("should succeed");

        assert_eq!(result.found.len(), 1);
        assert!(result.not_found_ids.is_empty());
        assert_eq!(result.found[&ki_id].principal_key, "user:alice");
    }

    #[tokio::test]
    async fn test_multiple_items_found() {
        let ki_id_1 = KnowledgeItemId::new();
        let ki_id_2 = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item_1 = test_item(ki_id_1, prin_id);
        let item_2 = test_item(ki_id_2, prin_id);
        let principal = test_principal(prin_id, "user:bob");

        let repos = repos_for_get_item(vec![item_1, item_2], vec![principal]);
        let result = call_execute(&repos, default_params(vec![ki_id_1, ki_id_2]))
            .await
            .expect("should succeed");

        assert_eq!(result.found.len(), 2);
        assert!(result.not_found_ids.is_empty());
    }

    #[tokio::test]
    async fn test_partial_not_found() {
        let ki_id_found = KnowledgeItemId::new();
        let ki_id_missing = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id_found, prin_id);
        let principal = test_principal(prin_id, "user:carol");

        let repos = repos_for_get_item(vec![item], vec![principal]);
        let result = call_execute(&repos, default_params(vec![ki_id_found, ki_id_missing]))
            .await
            .expect("should succeed");

        assert_eq!(result.found.len(), 1);
        assert!(result.found.contains_key(&ki_id_found));
        assert_eq!(result.not_found_ids.len(), 1);
        assert_eq!(result.not_found_ids[0], ki_id_missing.to_string());
    }

    #[tokio::test]
    async fn test_all_not_found() {
        let ki_id_1 = KnowledgeItemId::new();
        let ki_id_2 = KnowledgeItemId::new();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_ids(vec![], None)
            .build();
        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let result = call_execute(&repos, default_params(vec![ki_id_1, ki_id_2]))
            .await
            .expect("should succeed");

        assert!(result.found.is_empty());
        assert_eq!(result.not_found_ids.len(), 2);
    }

    // -- Service: enrichment -----------------------------------------------

    #[tokio::test]
    async fn test_include_standing_true() {
        let ki_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id, prin_id);
        let principal = test_principal(prin_id, "user:dave");
        let standing = a_standing().build();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_ids(vec![item], None)
            .build();
        let prin_mock = MockPrincipalRepository::builder()
            .on_find_by_ids(vec![principal], None)
            .build();
        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![standing.clone()], None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.principal = Arc::new(prin_mock);
        repos.standing = Arc::new(standing_mock);

        let params = GetItemParams {
            item_ids: vec![ki_id],
            include_standing: true,
            include_references: false,
        };
        let result = call_execute(&repos, params).await.expect("should succeed");

        assert!(result.found[&ki_id].standing.is_some());
        assert!(result.found[&ki_id].references.is_none());
    }

    #[tokio::test]
    async fn test_include_standing_false() {
        let ki_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id, prin_id);
        let principal = test_principal(prin_id, "user:eve");

        let repos = repos_for_get_item(vec![item], vec![principal]);
        let result = call_execute(&repos, default_params(vec![ki_id]))
            .await
            .expect("should succeed");

        assert!(result.found[&ki_id].standing.is_none());
    }

    #[tokio::test]
    async fn test_include_references_true() {
        let ki_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id, prin_id);
        let principal = test_principal(prin_id, "user:frank");
        let reference = a_reference().knowledge_item_id(ki_id).build();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_ids(vec![item], None)
            .build();
        let prin_mock = MockPrincipalRepository::builder()
            .on_find_by_ids(vec![principal], None)
            .build();
        let ref_mock = MockReferenceRepository::builder()
            .on_find_by_knowledge_item_ids(vec![reference], None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.principal = Arc::new(prin_mock);
        repos.reference = Arc::new(ref_mock);

        let params = GetItemParams {
            item_ids: vec![ki_id],
            include_standing: false,
            include_references: true,
        };
        let result = call_execute(&repos, params).await.expect("should succeed");

        assert!(result.found[&ki_id].references.is_some());
        assert_eq!(result.found[&ki_id].references.as_ref().unwrap().len(), 1);
        assert!(result.found[&ki_id].standing.is_none());
    }

    #[tokio::test]
    async fn test_include_references_false() {
        let ki_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id, prin_id);
        let principal = test_principal(prin_id, "user:grace");

        let repos = repos_for_get_item(vec![item], vec![principal]);
        let result = call_execute(&repos, default_params(vec![ki_id]))
            .await
            .expect("should succeed");

        assert!(result.found[&ki_id].references.is_none());
    }

    #[tokio::test]
    async fn test_include_standing_and_references_true() {
        let ki_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id, prin_id);
        let principal = test_principal(prin_id, "user:heidi");
        let standing = a_standing().build();
        let reference = a_reference().knowledge_item_id(ki_id).build();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_ids(vec![item], None)
            .build();
        let prin_mock = MockPrincipalRepository::builder()
            .on_find_by_ids(vec![principal], None)
            .build();
        let standing_mock = MockStandingRepository::builder()
            .on_compute(vec![standing], None)
            .build();
        let ref_mock = MockReferenceRepository::builder()
            .on_find_by_knowledge_item_ids(vec![reference], None)
            .build();

        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);
        repos.principal = Arc::new(prin_mock);
        repos.standing = Arc::new(standing_mock);
        repos.reference = Arc::new(ref_mock);

        let params = GetItemParams {
            item_ids: vec![ki_id],
            include_standing: true,
            include_references: true,
        };
        let result = call_execute(&repos, params).await.expect("should succeed");

        assert!(result.found[&ki_id].standing.is_some());
        assert!(result.found[&ki_id].references.is_some());
    }

    // -- Service: edge cases -----------------------------------------------

    #[tokio::test]
    async fn test_duplicate_ids_deduplicates_in_response() {
        let ki_id = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id, prin_id);
        let principal = test_principal(prin_id, "user:ivan");

        let repos = repos_for_get_item(vec![item], vec![principal]);
        let result = call_execute(&repos, default_params(vec![ki_id, ki_id]))
            .await
            .expect("should succeed");

        assert_eq!(result.found.len(), 1);
        assert!(result.found.contains_key(&ki_id));
        assert!(result.not_found_ids.is_empty());
    }

    #[tokio::test]
    async fn test_duplicate_not_found_ids_deduplicates() {
        let ki_id = KnowledgeItemId::new();

        let ki_mock = MockKnowledgeItemRepository::builder()
            .on_find_by_ids(vec![], None)
            .build();
        let mut repos = test_repositories();
        repos.knowledge_item = Arc::new(ki_mock);

        let result = call_execute(&repos, default_params(vec![ki_id, ki_id]))
            .await
            .expect("should succeed");

        assert!(result.found.is_empty());
        assert_eq!(result.not_found_ids.len(), 1);
        assert_eq!(result.not_found_ids[0], ki_id.to_string());
    }

    // -- Adapter: happy path -----------------------------------------------

    #[tokio::test]
    async fn test_apply_partial_not_found_builds_response_map() {
        let ki_id_found = KnowledgeItemId::new();
        let ki_id_missing = KnowledgeItemId::new();
        let prin_id = PrincipalId::new();
        let item = test_item(ki_id_found, prin_id);
        let principal = test_principal(prin_id, "user:ada");

        let repos = repos_for_get_item(vec![item], vec![principal]);
        let ctx = TestDb::new().await;
        let pool = ctx.create_pool().await.expect("pool");

        let found_str = ki_id_found.to_string();
        let missing_str = ki_id_missing.to_string();

        let handler = TestHandler::builder()
            .pool(pool)
            .repositories(repos)
            .build();

        let result = handler
            .apply_get_item(serde_json::json!({
                "item_ids": [found_str, missing_str],
            }))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert!(!structured["items"][&found_str].is_null());
        assert!(structured["items"][&missing_str].is_null());

        let RawContent::Text(t) = &result.content[0].raw else {
            panic!("expected text content");
        };
        assert!(
            t.text.contains("Retrieved 1 of 2 requested items."),
            "unexpected text: {}",
            t.text
        );
        assert!(
            t.text.contains(&format!("1 ID not found: {missing_str}")),
            "unexpected text: {}",
            t.text
        );
    }

    // -- Adapter: validation -----------------------------------------------

    #[tokio::test]
    async fn test_empty_item_ids_returns_application_error() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_get_item(serde_json::json!({"item_ids": []}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("at least 1"));
    }

    #[tokio::test]
    async fn test_too_many_item_ids_returns_application_error() {
        let handler = TestHandler::builder().build();

        let ids: Vec<String> = (0..21)
            .map(|_| KnowledgeItemId::new().to_string())
            .collect();

        let result = handler
            .apply_get_item(serde_json::json!({"item_ids": ids}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains(&format!("at most {MAX_ITEM_IDS}")));
    }

    #[tokio::test]
    async fn test_invalid_prefix_returns_application_error() {
        let handler = TestHandler::builder().build();
        let wrong_id = ProjectId::new().to_string();

        let result = handler
            .apply_get_item(serde_json::json!({"item_ids": [wrong_id]}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("expected prefix"));
    }

    #[tokio::test]
    async fn test_malformed_json_returns_protocol_error() {
        let handler = TestHandler::builder().build();

        let err = handler
            .apply_get_item(serde_json::json!({"item_ids": 123}))
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }
}
