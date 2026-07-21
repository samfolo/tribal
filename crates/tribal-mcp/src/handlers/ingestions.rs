//! Resource reads for the principal-scoped ingestion history:
//! `tribal://ingestions/recent` and `tribal://ingestions/{job_id}/input`.
//!
//! Both are application-controlled context rather than model-selected
//! actions, so they are resources, not tools. Content flows only under
//! knowledge authority, and every read is principal-qualified before it
//! touches anything else.

use std::{str::FromStr, sync::LazyLock};

use rmcp::model::{
    ErrorData as McpError, RawResourceTemplate, ReadResourceResult, ResourceContents,
    ResourceTemplate,
};
use tribal_auth::AuthenticatedPrincipal;
use tribal_db::{DbError, RecentIngestionCursor, RecentIngestionsQuery};
use tribal_domain::{JobId, JobStatus, PrincipalId, Scope};

use crate::{
    contract::{
        McpResourceRead,
        declarations::{McpIngestionInputResource, McpRecentIngestionsResource},
    },
    mapping::{McpIngestionInputResponse, McpRecentIngestionsResponse, recent_ingestion_to_wire},
    server_handler::TribalServerHandler,
};

// ---------------------------------------------------------------------------
// Contract surface
// ---------------------------------------------------------------------------

/// URI template of the bounded recent-ingestion listing.
pub(crate) const RECENT_INGESTIONS_URI_TEMPLATE: &str =
    "tribal://ingestions/recent{?project_id,statuses,cursor,limit}";

/// URI template of the full-input read.
pub(crate) const INGESTION_INPUT_URI_TEMPLATE: &str = "tribal://ingestions/{job_id}/input";

/// Scope both history reads require: previews and raw input are content.
pub(crate) const INGESTIONS_REQUIRED_SCOPE: &str = "tribal.knowledge:read";

/// The required scope, parsed once.
pub(crate) static INGESTIONS_SCOPE: LazyLock<Scope> = LazyLock::new(|| {
    INGESTIONS_REQUIRED_SCOPE
        .parse()
        .expect("invariant: resource required scope must be a valid scope")
});

/// Base URI (before the query) of the recent listing.
const RECENT_INGESTIONS_URI: &str = "tribal://ingestions/recent";

/// Prefix and suffix bounding the input read's `job_id` segment.
const INGESTION_URI_PREFIX: &str = "tribal://ingestions/";
const INGESTION_INPUT_URI_SUFFIX: &str = "/input";

/// Rows returned when the caller names no limit.
const RECENT_LIMIT_DEFAULT: u16 = 20;

/// The two templated resources, advertised as their declarations state.
pub(crate) fn ingestion_resource_templates() -> Vec<ResourceTemplate> {
    vec![
        resource_template::<McpRecentIngestionsResource>(),
        resource_template::<McpIngestionInputResource>(),
    ]
}

/// Projects one resource declaration into its advertised template.
fn resource_template<R: McpResourceRead>() -> ResourceTemplate {
    ResourceTemplate {
        raw: RawResourceTemplate {
            uri_template: R::URI_TEMPLATE.to_owned(),
            name: R::PRESENTATION.name.to_owned(),
            title: Some(R::PRESENTATION.title.to_owned()),
            description: Some(R::PRESENTATION.description.to_owned()),
            mime_type: Some(R::PRESENTATION.mime_type.to_owned()),
            icons: None,
        },
        annotations: None,
    }
}

/// Whether a URI addresses one of the ingestion resources.
pub(crate) fn is_ingestion_uri(uri: &str) -> bool {
    uri.starts_with(RECENT_INGESTIONS_URI) || parse_input_uri(uri).is_some()
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

impl TribalServerHandler {
    /// Serves an ingestion-resource read: scope first, then the
    /// principal-qualified query, so neither foreign existence nor
    /// content ever leaves the boundary.
    pub(crate) async fn read_ingestion_resource(
        &self,
        uri: &str,
        principal: &AuthenticatedPrincipal,
    ) -> Result<ReadResourceResult, McpError> {
        if !tribal_domain::is_authorised(principal.scopes(), &INGESTIONS_SCOPE) {
            return Err(McpError::invalid_request(
                format!("resource requires the {INGESTIONS_REQUIRED_SCOPE} scope"),
                None,
            ));
        }
        let principal_id = principal.principal_id();

        if let Some(job_id) = parse_input_uri(uri) {
            return self.read_ingestion_input(uri, job_id, principal_id).await;
        }
        if uri == RECENT_INGESTIONS_URI || uri.starts_with("tribal://ingestions/recent?") {
            let query = parse_recent_query(uri)?;
            return self.read_recent_ingestions(uri, query, principal_id).await;
        }
        Err(McpError::invalid_params("unknown resource URI", None))
    }

    async fn read_recent_ingestions(
        &self,
        uri: &str,
        query: RecentIngestionsQuery,
        principal_id: PrincipalId,
    ) -> Result<ReadResourceResult, McpError> {
        let mut conn = self.ingestion_read_connection().await?;
        let page = self
            .repositories
            .job
            .list_recent_for_principal(&mut conn, principal_id, &query)
            .await
            .map_err(db_read_error)?;

        let response = McpRecentIngestionsResponse {
            ingestions: page
                .ingestions
                .into_iter()
                .map(recent_ingestion_to_wire)
                .collect(),
            next_cursor: page.next_cursor.map(|cursor| cursor.encode()),
        };
        json_resource(uri, &response)
    }

    async fn read_ingestion_input(
        &self,
        uri: &str,
        job_id: JobId,
        principal_id: PrincipalId,
    ) -> Result<ReadResourceResult, McpError> {
        let mut conn = self.ingestion_read_connection().await?;
        let job = self
            .repositories
            .job
            .find_by_id_for_principal(&mut conn, job_id, principal_id)
            .await
            .map_err(db_read_error)?;

        let response = McpIngestionInputResponse {
            job_id: job.id().to_string(),
            content: job.raw_input().to_owned(),
        };
        json_resource(uri, &response)
    }
}

impl TribalServerHandler {
    /// Acquires a pool connection for a resource read, whose failure
    /// channel is the protocol error rather than a tool result.
    async fn ingestion_read_connection(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, McpError> {
        self.state
            .pool_mcp
            .acquire()
            .await
            .map_err(|e| McpError::internal_error(format!("acquiring connection: {e}"), None))
    }
}

// ---------------------------------------------------------------------------
// URI parsing
// ---------------------------------------------------------------------------

/// Extracts the job id from an input-read URI, `None` when the URI is
/// not that template's shape.
fn parse_input_uri(uri: &str) -> Option<JobId> {
    let segment = uri
        .strip_prefix(INGESTION_URI_PREFIX)?
        .strip_suffix(INGESTION_INPUT_URI_SUFFIX)?;
    if segment.is_empty() || segment.contains('/') {
        return None;
    }
    segment.parse::<JobId>().ok()
}

/// Parses the recent listing's query parameters: RFC 6570 form-style,
/// with `statuses` as one non-exploded comma-separated list.
fn parse_recent_query(uri: &str) -> Result<RecentIngestionsQuery, McpError> {
    let mut query = RecentIngestionsQuery {
        limit: RECENT_LIMIT_DEFAULT,
        ..RecentIngestionsQuery::default()
    };
    let Some((_, raw_query)) = uri.split_once('?') else {
        return Ok(query);
    };

    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        match name {
            "project_id" => {
                query.project_id = Some(value.parse().map_err(|_| {
                    McpError::invalid_params("project_id is not a project id", None)
                })?);
            }
            "statuses" => {
                query.statuses = value
                    .split(',')
                    .filter(|status| !status.is_empty())
                    .map(|status| {
                        JobStatus::from_str(status).map_err(|_| {
                            McpError::invalid_params(
                                format!("unknown status value: {status}"),
                                None,
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "cursor" => {
                query.before = Some(RecentIngestionCursor::decode(value).map_err(|_| {
                    McpError::invalid_params("cursor is not a recent-ingestion cursor", None)
                })?);
            }
            "limit" => {
                query.limit = value
                    .parse()
                    .map_err(|_| McpError::invalid_params("limit is not a number", None))?;
            }
            other => {
                return Err(McpError::invalid_params(
                    format!("unknown query parameter: {other}"),
                    None,
                ));
            }
        }
    }
    Ok(query)
}

// ---------------------------------------------------------------------------
// Result shaping
// ---------------------------------------------------------------------------

/// Serialises a response body as the resource's JSON contents.
fn json_resource<T: serde::Serialize>(uri: &str, body: &T) -> Result<ReadResourceResult, McpError> {
    let text = serde_json::to_string(body)
        .map_err(|e| McpError::internal_error(format!("serialising resource: {e}"), None))?;
    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type("application/json"),
    ]))
}

/// Maps a repository failure without disclosing whether a foreign row
/// exists: missing and foreign are one resource-not-found.
fn db_read_error(error: DbError) -> McpError {
    match error {
        DbError::NotFound { .. } => McpError::resource_not_found("no such ingestion", None),
        other => McpError::internal_error(format!("reading ingestions: {other}"), None),
    }
}

#[cfg(test)]
mod tests {
    use rmcp::{
        handler::server::ServerHandler,
        model::{ErrorCode, Extensions as RmcpExtensions, ReadResourceResult, ResourceContents},
    };
    use sqlx::PgConnection;
    use tribal_auth::AuthContext;
    use tribal_db::{
        JobRepository, NewJob, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
        PrincipalRepository, ProjectRepository,
    };
    use tribal_domain::{GitRemote, JobOutcome, ProjectId, PromptVersionId};
    use tribal_test_utils::{
        TestDb, a_job_status_transition, a_new_job, a_new_principal, a_new_project,
        a_new_prompt_version, a_new_system_fingerprint, insert_prompt_version,
        upsert_system_fingerprint,
    };

    use super::*;
    use crate::{
        mapping::McpRecentIngestionsResponse,
        server_handler::ConnectionRepositories,
        test_utils::{TestHandler, test_request_context},
    };

    // -- Helpers -------------------------------------------------------

    /// Inserts a principal, project, prompt version, and system
    /// fingerprint via the real repositories, returning IDs a real job
    /// insert can bind against.
    async fn setup_ingestion_prerequisites(
        conn: &mut PgConnection,
        suffix: &str,
    ) -> (PrincipalId, ProjectId, PromptVersionId, String) {
        let principal = PgPrincipalRepository
            .insert(
                conn,
                &a_new_principal()
                    .principal_key(format!("user:ingestions-{suffix}"))
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
                        &format!("test/ingestions-{suffix}"),
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("insert project");
        let pv_id = insert_prompt_version(conn, &a_new_prompt_version().build()).await;
        let fingerprint_hash =
            upsert_system_fingerprint(conn, &a_new_system_fingerprint().build()).await;
        (principal.id(), project.id(), pv_id, fingerprint_hash)
    }

    /// Builds a `NewJob` bound to the given prerequisites, with
    /// distinguishing raw-input content.
    fn a_job_for(
        project_id: ProjectId,
        principal_id: PrincipalId,
        pv_id: PromptVersionId,
        fingerprint_hash: &str,
        content: &str,
    ) -> NewJob {
        a_new_job()
            .project_id(project_id)
            .principal_id(principal_id)
            .raw_input(content.to_owned())
            .extraction_system_prompt_version_id(pv_id)
            .extraction_user_prompt_version_id(pv_id)
            .triage_system_prompt_version_id(pv_id)
            .triage_user_prompt_version_id(pv_id)
            .relation_system_prompt_version_id(pv_id)
            .relation_user_prompt_version_id(pv_id)
            .system_fingerprint_hash(fingerprint_hash.to_owned())
            .build()
    }

    fn knowledge_read_scope() -> Scope {
        "tribal.knowledge:read".parse().expect("valid scope")
    }

    fn jobs_read_scope() -> Scope {
        "tribal.jobs:read".parse().expect("valid scope")
    }

    /// Decodes a `tribal://ingestions/recent` read into its wire response.
    fn recent_ingestions_response(result: &ReadResourceResult) -> McpRecentIngestionsResponse {
        let ResourceContents::TextResourceContents { text, .. } = &result.contents[0] else {
            panic!("expected text resource content");
        };
        serde_json::from_str(text).expect("recent-ingestions response deserialises")
    }

    // -- Handler: recent-ingestions reads through a real database -------

    #[tokio::test]
    async fn test_a_knowledge_read_principal_reads_only_its_own_rows_newest_first_with_no_further_page()
     {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("conn");
        let (principal_id, project_id, pv_id, fp_hash) =
            setup_ingestion_prerequisites(&mut conn, "own-rows").await;
        let (stranger_id, stranger_project, stranger_pv, stranger_fp) =
            setup_ingestion_prerequisites(&mut conn, "own-rows-stranger").await;

        let repo = PgJobRepository;
        let older = repo
            .insert(
                &mut conn,
                &a_job_for(project_id, principal_id, pv_id, &fp_hash, "older note"),
            )
            .await
            .expect("insert older job");
        let newer = repo
            .insert(
                &mut conn,
                &a_job_for(project_id, principal_id, pv_id, &fp_hash, "newer note"),
            )
            .await
            .expect("insert newer job");
        let foreign = repo
            .insert(
                &mut conn,
                &a_job_for(
                    stranger_project,
                    stranger_id,
                    stranger_pv,
                    &stranger_fp,
                    "foreign note",
                ),
            )
            .await
            .expect("insert foreign job");
        drop(conn);

        let handler = TestHandler::builder()
            .pool(ctx.pool().clone())
            .repositories(ConnectionRepositories::new())
            .build();
        let principal = AuthenticatedPrincipal::for_test(
            principal_id,
            "user:own-rows",
            vec![knowledge_read_scope()],
        );

        let result = handler
            .read_ingestion_resource(RECENT_INGESTIONS_URI, &principal)
            .await
            .expect("read must succeed");
        let response = recent_ingestions_response(&result);

        assert_eq!(
            response.ingestions.len(),
            2,
            "only the principal's own jobs list"
        );
        let ids: Vec<&str> = response
            .ingestions
            .iter()
            .map(|i| i.job_id.as_str())
            .collect();
        assert!(ids.contains(&newer.id().to_string().as_str()));
        assert!(ids.contains(&older.id().to_string().as_str()));
        assert!(
            !ids.contains(&foreign.id().to_string().as_str()),
            "a foreign job must never list"
        );
        assert!(
            response.ingestions[0].created_at >= response.ingestions[1].created_at,
            "newest first",
        );
        assert!(response.next_cursor.is_none(), "the page covers everything");
    }

    #[tokio::test]
    async fn test_the_cursor_from_a_page_of_one_fetches_the_remaining_row() {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("conn");
        let (principal_id, project_id, pv_id, fp_hash) =
            setup_ingestion_prerequisites(&mut conn, "cursor").await;
        let repo = PgJobRepository;
        let first = repo
            .insert(
                &mut conn,
                &a_job_for(project_id, principal_id, pv_id, &fp_hash, "first"),
            )
            .await
            .expect("insert first job");
        let second = repo
            .insert(
                &mut conn,
                &a_job_for(project_id, principal_id, pv_id, &fp_hash, "second"),
            )
            .await
            .expect("insert second job");
        drop(conn);

        let handler = TestHandler::builder()
            .pool(ctx.pool().clone())
            .repositories(ConnectionRepositories::new())
            .build();
        let principal = AuthenticatedPrincipal::for_test(
            principal_id,
            "user:cursor",
            vec![knowledge_read_scope()],
        );

        let page_one = handler
            .read_ingestion_resource(&format!("{RECENT_INGESTIONS_URI}?limit=1"), &principal)
            .await
            .expect("first page reads");
        let response_one = recent_ingestions_response(&page_one);
        assert_eq!(response_one.ingestions.len(), 1);
        let cursor = response_one
            .next_cursor
            .clone()
            .expect("a further page exists");

        let page_two = handler
            .read_ingestion_resource(
                &format!("{RECENT_INGESTIONS_URI}?cursor={cursor}&limit=1"),
                &principal,
            )
            .await
            .expect("second page reads");
        let response_two = recent_ingestions_response(&page_two);
        assert_eq!(response_two.ingestions.len(), 1);
        assert_ne!(
            response_one.ingestions[0].job_id, response_two.ingestions[0].job_id,
            "the second page returns the other row",
        );
        assert!(
            response_two.next_cursor.is_none(),
            "the second page is the last"
        );

        let seen: std::collections::HashSet<&str> = [
            response_one.ingestions[0].job_id.as_str(),
            response_two.ingestions[0].job_id.as_str(),
        ]
        .into_iter()
        .collect();
        assert!(seen.contains(first.id().to_string().as_str()));
        assert!(seen.contains(second.id().to_string().as_str()));
    }

    #[tokio::test]
    async fn test_statuses_filters_the_listing_and_an_unknown_status_is_refused() {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("conn");
        let (principal_id, project_id, pv_id, fp_hash) =
            setup_ingestion_prerequisites(&mut conn, "status-filter").await;
        let repo = PgJobRepository;
        let queued = repo
            .insert(
                &mut conn,
                &a_job_for(project_id, principal_id, pv_id, &fp_hash, "queued note"),
            )
            .await
            .expect("insert queued job");
        let completed = repo
            .insert(
                &mut conn,
                &a_job_for(project_id, principal_id, pv_id, &fp_hash, "completed note"),
            )
            .await
            .expect("insert completing job");
        repo.update_status_if_live(
            &mut conn,
            completed.id(),
            &a_job_status_transition()
                .status(JobStatus::Completed)
                .outcome(Some(JobOutcome::Success))
                .completed_at(Some(chrono::Utc::now()))
                .build(),
        )
        .await
        .expect("complete job");
        drop(conn);

        let handler = TestHandler::builder()
            .pool(ctx.pool().clone())
            .repositories(ConnectionRepositories::new())
            .build();
        let principal = AuthenticatedPrincipal::for_test(
            principal_id,
            "user:status-filter",
            vec![knowledge_read_scope()],
        );

        let both = handler
            .read_ingestion_resource(
                &format!("{RECENT_INGESTIONS_URI}?statuses=queued,completed"),
                &principal,
            )
            .await
            .expect("valid statuses read");
        assert_eq!(recent_ingestions_response(&both).ingestions.len(), 2);

        let completed_only = handler
            .read_ingestion_resource(
                &format!("{RECENT_INGESTIONS_URI}?statuses=completed"),
                &principal,
            )
            .await
            .expect("filtered read");
        let response = recent_ingestions_response(&completed_only);
        assert_eq!(response.ingestions.len(), 1);
        assert_eq!(response.ingestions[0].job_id, completed.id().to_string());
        assert_ne!(response.ingestions[0].job_id, queued.id().to_string());

        let err = handler
            .read_ingestion_resource(
                &format!("{RECENT_INGESTIONS_URI}?statuses=queued,nonsense"),
                &principal,
            )
            .await
            .expect_err("an unknown status value must be refused");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -- Handler: input reads and non-disclosure -------------------------

    #[tokio::test]
    async fn test_a_foreign_job_and_a_missing_job_fail_the_input_read_identically() {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("conn");
        let (owner_id, project_id, pv_id, fp_hash) =
            setup_ingestion_prerequisites(&mut conn, "non-disclosure-owner").await;
        let (stranger_id, _, _, _) =
            setup_ingestion_prerequisites(&mut conn, "non-disclosure-stranger").await;
        let foreign_job = PgJobRepository
            .insert(
                &mut conn,
                &a_job_for(project_id, owner_id, pv_id, &fp_hash, "owner-only content"),
            )
            .await
            .expect("insert foreign job");
        drop(conn);

        let handler = TestHandler::builder()
            .pool(ctx.pool().clone())
            .repositories(ConnectionRepositories::new())
            .build();
        let stranger = AuthenticatedPrincipal::for_test(
            stranger_id,
            "user:stranger",
            vec![knowledge_read_scope()],
        );

        let foreign_uri = format!("tribal://ingestions/{}/input", foreign_job.id());
        let missing_uri = format!("tribal://ingestions/{}/input", JobId::new());

        let foreign_err = handler
            .read_ingestion_resource(&foreign_uri, &stranger)
            .await
            .expect_err("a foreign job must not be readable");
        let missing_err = handler
            .read_ingestion_resource(&missing_uri, &stranger)
            .await
            .expect_err("a missing job must fail the same way");

        assert_eq!(
            foreign_err, missing_err,
            "foreign and missing must be indistinguishable",
        );
        assert_eq!(foreign_err.code, ErrorCode::RESOURCE_NOT_FOUND);
    }

    // -- Handler: scope enforcement ---------------------------------------

    #[tokio::test]
    async fn test_a_principal_without_knowledge_read_is_refused_the_recent_listing() {
        let handler = TestHandler::builder().build();
        let principal = AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:jobs-only",
            vec![jobs_read_scope()],
        );

        let err = handler
            .read_ingestion_resource(RECENT_INGESTIONS_URI, &principal)
            .await
            .expect_err("jobs:read alone must not grant the ingestion resources");

        assert_eq!(err.code, ErrorCode::INVALID_REQUEST);
        assert!(
            err.message.contains(INGESTIONS_REQUIRED_SCOPE),
            "the refusal must name the missing scope: {}",
            err.message,
        );
    }

    // -- ServerHandler: resource template advertisement -------------------

    #[tokio::test]
    async fn test_list_resource_templates_advertises_the_ingestion_templates_to_a_knowledge_read_principal()
     {
        let principal = AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:templates-knowledge",
            vec![knowledge_read_scope()],
        );
        let handler = TestHandler::builder()
            .auth(AuthContext::new(principal))
            .build();
        let context = test_request_context(RmcpExtensions::new());

        let result = handler
            .list_resource_templates(None, context)
            .await
            .expect("must succeed");

        assert_eq!(result.resource_templates.len(), 2);
        let names: Vec<String> = result
            .resource_templates
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert!(names.contains(&"recent_ingestions".to_owned()));
        assert!(names.contains(&"ingestion_input".to_owned()));
    }

    #[tokio::test]
    async fn test_list_resource_templates_advertises_nothing_to_a_jobs_read_only_principal() {
        let principal = AuthenticatedPrincipal::for_test(
            PrincipalId::new(),
            "user:templates-jobs",
            vec![jobs_read_scope()],
        );
        let handler = TestHandler::builder()
            .auth(AuthContext::new(principal))
            .build();
        let context = test_request_context(RmcpExtensions::new());

        let result = handler
            .list_resource_templates(None, context)
            .await
            .expect("must succeed");

        assert!(result.resource_templates.is_empty());
    }

    // -- URI and query parsing ---------------------------------------------

    #[test]
    fn test_the_input_uri_parses_only_its_exact_shape() {
        let id = JobId::new();
        let uri = format!("tribal://ingestions/{id}/input");

        assert_eq!(parse_input_uri(&uri), Some(id));
        assert_eq!(parse_input_uri("tribal://ingestions//input"), None);
        assert_eq!(parse_input_uri("tribal://ingestions/recent"), None);
        assert_eq!(parse_input_uri("tribal://ingestions/x/y/input"), None);
        assert_eq!(parse_input_uri("tribal://ingestions/not-a-job/input"), None);
    }

    #[test]
    fn test_the_recent_query_parses_the_settled_encoding() {
        let query =
            parse_recent_query("tribal://ingestions/recent?statuses=queued,completed&limit=5")
                .expect("query parses");

        assert_eq!(
            query.statuses,
            vec![JobStatus::Queued, JobStatus::Completed]
        );
        assert_eq!(query.limit, 5);
        assert!(query.project_id.is_none());
        assert!(query.before.is_none());
    }

    #[test]
    fn test_an_unknown_status_value_is_refused() {
        let err = parse_recent_query("tribal://ingestions/recent?statuses=queued,telepathic")
            .unwrap_err();

        assert!(err.message.contains("unknown status value: telepathic"));
    }

    #[test]
    fn test_an_unknown_query_parameter_is_refused() {
        assert!(parse_recent_query("tribal://ingestions/recent?principal_id=abc").is_err());
    }

    #[test]
    fn test_an_absent_query_takes_the_default_limit() {
        let query = parse_recent_query("tribal://ingestions/recent").expect("query parses");

        assert_eq!(query.limit, RECENT_LIMIT_DEFAULT);
        assert!(query.statuses.is_empty());
    }
}
