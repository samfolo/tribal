use sqlx::{PgPool, Row};
use wiremock::MockServer;

// ---------------------------------------------------------------------------
// DiagnosticContext
// ---------------------------------------------------------------------------

/// Collects diagnostic information when a job does not complete as expected.
pub struct DiagnosticContext<'a> {
    pub pool: &'a PgPool,
    pub embedding_server: &'a MockServer,
    pub extraction_server: &'a MockServer,
    pub triage_server: &'a MockServer,
    pub relation_server: &'a MockServer,
}

impl DiagnosticContext<'_> {
    /// Formats a failure diagnostic for the given job.
    pub async fn format_failure(&self, job_id: &str, status: &str) -> String {
        let mut output =
            format!("Job {job_id} did not complete successfully.\n  Status: {status}\n");

        // -- Task statuses ---------------------------------------------------
        let task_info = self.query_tasks(job_id).await;
        if !task_info.is_empty() {
            output.push_str("\n  Task statuses:\n");
            output.push_str(&task_info);
        }

        // -- Wiremock request counts -----------------------------------------
        let embed_count = request_count(self.embedding_server).await;
        let extract_count = request_count(self.extraction_server).await;
        let triage_count = request_count(self.triage_server).await;
        let relation_count = request_count(self.relation_server).await;

        output.push_str("\n  Wiremock request counts:\n");
        output.push_str(&format!("    embedding: {embed_count} calls\n"));
        output.push_str(&format!("    extraction: {extract_count} calls\n"));
        output.push_str(&format!("    triage: {triage_count} calls\n"));
        output.push_str(&format!("    relation: {relation_count} calls\n"));

        // -- Request excerpts for the busiest server -------------------------
        let servers = [
            ("triage", self.triage_server, triage_count),
            ("extraction", self.extraction_server, extract_count),
            ("relation", self.relation_server, relation_count),
            ("embedding", self.embedding_server, embed_count),
        ];
        if let Some((name, server, _)) = servers.iter().max_by_key(|(_, _, c)| c) {
            let excerpts = request_excerpts(server, 5).await;
            if !excerpts.is_empty() {
                output.push_str(&format!("\n  Wiremock request excerpts ({name}, last 5):\n"));
                for (i, excerpt) in excerpts.iter().enumerate() {
                    output.push_str(&format!("    [{}] {excerpt}\n", i + 1));
                }
            }
        }

        output
    }

    async fn query_tasks(&self, job_id: &str) -> String {
        let uuid_str = job_id.strip_prefix("ji_").unwrap_or(job_id);

        let rows = sqlx::query(
            "SELECT stage::text, batch_index, status::text, \
             error_kind, error_message, retry_count \
             FROM tasks WHERE job_id = $1::uuid \
             ORDER BY stage, batch_index",
        )
        .bind(uuid_str)
        .fetch_all(self.pool)
        .await
        .unwrap_or_default();

        let mut output = String::new();
        for row in &rows {
            let stage: &str = row.get("stage");
            let batch_index: i32 = row.get("batch_index");
            let status: &str = row.get("status");
            output.push_str(&format!("    {stage}_{batch_index}: {status}"));

            let error_kind: Option<&str> = row.try_get("error_kind").ok().flatten();
            if let Some(ek) = error_kind {
                output.push_str(&format!("\n      error_kind: {ek}"));
            }
            let error_message: Option<&str> = row.try_get("error_message").ok().flatten();
            if let Some(em) = error_message {
                output.push_str(&format!("\n      error_message: \"{em}\""));
            }
            let retry_count: i32 = row.get("retry_count");
            if retry_count > 0 {
                output.push_str(&format!("\n      retry_count: {retry_count}"));
            }
            output.push('\n');
        }
        output
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn request_count(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .map_or(0, |r| r.len())
}

async fn request_excerpts(server: &MockServer, limit: usize) -> Vec<String> {
    let Some(requests) = server.received_requests().await else {
        return Vec::new();
    };

    requests
        .iter()
        .rev()
        .take(limit)
        .map(|r| {
            let method = &r.method;
            let path = r.url.path();
            let body = String::from_utf8_lossy(&r.body);
            let excerpt: String = body.chars().take(200).collect();
            format!("{method} {path} — body excerpt: \"{excerpt}\"")
        })
        .collect()
}
