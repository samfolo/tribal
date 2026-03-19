use std::{fmt::Write, str::FromStr};

use sqlx::PgPool;
use tribal_db::{PgTaskRepository, TaskRepository};
use tribal_domain::{JobId, Task};
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
    pub async fn format_failure(&self, job_id: &str, status: &str, outcome: &str) -> String {
        let outcome_display = if outcome.is_empty() { "n/a" } else { outcome };
        let mut out = format!(
            "Job {job_id} did not complete successfully.\
             \n  Status: {status}\
             \n  Outcome: {outcome_display}\n",
        );

        self.append_task_statuses(&mut out, job_id).await;
        self.append_wiremock_summary(&mut out).await;

        out
    }

    async fn append_task_statuses(&self, out: &mut String, job_id: &str) {
        let Ok(parsed_id) = JobId::from_str(job_id) else {
            writeln!(out, "\n  (could not parse job ID for task lookup)").unwrap();
            return;
        };

        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                writeln!(out, "\n  (could not acquire connection: {e})").unwrap();
                return;
            }
        };

        let tasks = match PgTaskRepository.find_by_job_id(&mut conn, parsed_id).await {
            Ok(t) => t,
            Err(e) => {
                writeln!(out, "\n  (task query failed: {e})").unwrap();
                return;
            }
        };

        if tasks.is_empty() {
            writeln!(out, "\n  No tasks found for this job.").unwrap();
            return;
        }

        writeln!(out, "\n  Task statuses:").unwrap();
        for task in &tasks {
            format_task(out, task);
        }
    }

    async fn append_wiremock_summary(&self, out: &mut String) {
        let servers: [(&str, &MockServer); 4] = [
            ("embedding", self.embedding_server),
            ("extraction", self.extraction_server),
            ("triage", self.triage_server),
            ("relation", self.relation_server),
        ];

        writeln!(out, "\n  Wiremock request counts:").unwrap();
        for (name, server) in &servers {
            writeln!(out, "    {name}: {} calls", request_count(server).await).unwrap();
        }

        for (name, server) in &servers {
            let excerpts = request_excerpts(server, 5).await;
            if !excerpts.is_empty() {
                writeln!(out, "\n  Wiremock request excerpts ({name}, last 5):").unwrap();
                for (i, excerpt) in excerpts.iter().enumerate() {
                    writeln!(out, "    [{}] {excerpt}", i + 1).unwrap();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Task formatting
// ---------------------------------------------------------------------------

fn format_task(out: &mut String, task: &Task) {
    let batch = task.batch_index().unwrap_or(0);
    write!(out, "    {}_{batch}: {}", task.task_type(), task.status()).unwrap();

    if let Some(error_kind) = task.error_kind() {
        write!(out, "\n      error_kind: {error_kind}").unwrap();
    }
    if let Some(error_message) = task.error_message() {
        write!(out, "\n      error_message: \"{error_message}\"").unwrap();
    }
    if task.retry_count() > 0 {
        write!(out, "\n      retry_count: {}", task.retry_count()).unwrap();
    }
    if let Some(claimed_by) = task.claimed_by() {
        write!(out, "\n      claimed_by: {claimed_by}").unwrap();
    }
    write!(out, "\n      available_at: {}", task.available_at()).unwrap();
    if let Some(heartbeat_at) = task.heartbeat_at() {
        write!(out, "\n      heartbeat_at: {heartbeat_at}").unwrap();
    }
    out.push('\n');
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn request_count(server: &MockServer) -> usize {
    server.received_requests().await.map_or(0, |r| r.len())
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
