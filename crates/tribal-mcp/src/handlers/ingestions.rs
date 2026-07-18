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
    mapping::{
        McpIngestionInputResponse, McpRecentIngestionsResponse, recent_ingestion_to_wire,
    },
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

/// The two templated resources, for scope-filtered advertisement.
pub(crate) fn ingestion_resource_templates() -> Vec<ResourceTemplate> {
    let recent = RawResourceTemplate {
        uri_template: RECENT_INGESTIONS_URI_TEMPLATE.to_owned(),
        name: "recent_ingestions".to_owned(),
        title: Some("Recent ingestions".to_owned()),
        description: Some(
            "The caller's recent ingestion jobs, newest first, with bounded previews".to_owned(),
        ),
        mime_type: Some("application/json".to_owned()),
        icons: None,
    };
    let input = RawResourceTemplate {
        uri_template: INGESTION_INPUT_URI_TEMPLATE.to_owned(),
        name: "ingestion_input".to_owned(),
        title: Some("Ingestion input".to_owned()),
        description: Some("The verbatim content of one of the caller's ingestions".to_owned()),
        mime_type: Some("application/json".to_owned()),
        icons: None,
    };
    vec![
        ResourceTemplate {
            raw: recent,
            annotations: None,
        },
        ResourceTemplate {
            raw: input,
            annotations: None,
        },
    ]
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
                query.project_id =
                    Some(value.parse().map_err(|_| {
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
    use super::*;

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
        let query = parse_recent_query(
            "tribal://ingestions/recent?statuses=queued,completed&limit=5",
        )
        .expect("query parses");

        assert_eq!(query.statuses, vec![JobStatus::Queued, JobStatus::Completed]);
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
