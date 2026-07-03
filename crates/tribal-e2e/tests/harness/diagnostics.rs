use std::{fmt::Write, str::FromStr};

use serde_json::Value;
use sqlx::PgPool;
use tribal_db::{PgTaskRepository, TaskRepository};
use tribal_domain::{JobId, ProviderKind, Task};
use tribal_inference::RequestClass;
use tribal_test_utils::text::truncate;
use wiremock::MockServer;

// ---------------------------------------------------------------------------
// Excerpt limit
// ---------------------------------------------------------------------------

/// Maximum characters shown in a diagnostic request excerpt.
const EXCERPT_MAX_CHARS: usize = 200;

// ---------------------------------------------------------------------------
// ServerDiagnostic
// ---------------------------------------------------------------------------

/// A wiremock server paired with the metadata needed for diagnostic
/// formatting.
pub struct ServerDiagnostic<'a> {
    pub name: &'static str,
    pub server: &'a MockServer,
    pub provider: ProviderKind,
    pub request_class: RequestClass,
}

// ---------------------------------------------------------------------------
// DiagnosticContext
// ---------------------------------------------------------------------------

/// Collects diagnostic information when a job does not complete as expected.
pub struct DiagnosticContext<'a> {
    pub pool: &'a PgPool,
    pub servers: Vec<ServerDiagnostic<'a>>,
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
        writeln!(out, "\n  Wiremock request counts:").unwrap();
        for sd in &self.servers {
            writeln!(
                out,
                "    {}: {} calls",
                sd.name,
                request_count(sd.server).await,
            )
            .unwrap();
        }

        for sd in &self.servers {
            let excerpts = request_excerpts(sd.server, sd.provider, sd.request_class, 5).await;
            if !excerpts.is_empty() {
                writeln!(out, "\n  Wiremock request excerpts ({}, last 5):", sd.name).unwrap();
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
// Request excerpt formatting
// ---------------------------------------------------------------------------

async fn request_count(server: &MockServer) -> usize {
    server.received_requests().await.map_or(0, |r| r.len())
}

async fn request_excerpts(
    server: &MockServer,
    provider: ProviderKind,
    class: RequestClass,
    limit: usize,
) -> Vec<String> {
    let Some(requests) = server.received_requests().await else {
        return Vec::new();
    };

    requests
        .iter()
        .rev()
        .take(limit)
        .map(|r| format_request_excerpt(r, provider, class))
        .collect()
}

/// Formats a wiremock request into a compact, diagnostic-friendly string.
///
/// Uses the provider kind and request class to extract the
/// test-relevant payload (user content for inference, input text
/// for embedding) rather than showing the entire JSON body.
fn format_request_excerpt(
    r: &wiremock::Request,
    provider: ProviderKind,
    class: RequestClass,
) -> String {
    let method = &r.method;
    let path = r.url.path();

    if r.body.is_empty() {
        return format!("{method} {path}");
    }

    let body = String::from_utf8_lossy(&r.body);
    let Ok(json) = serde_json::from_str::<Value>(&body) else {
        return format!(
            "{method} {path} — body: \"{}\"",
            truncate(&body, EXCERPT_MAX_CHARS)
        );
    };

    let model = json["model"].as_str().unwrap_or("<nil>");

    match class {
        RequestClass::Inference => {
            let content = json["messages"]
                .as_array()
                .and_then(|msgs| msgs.last())
                .and_then(|msg| msg["content"].as_str())
                .unwrap_or("<nil>");
            format!(
                "{method} {path} — model: {model}, content: \"{}\"",
                truncate(content, EXCERPT_MAX_CHARS),
            )
        }
        RequestClass::Embedding => {
            let input = extract_embed_input(&json, provider);
            format!(
                "{method} {path} — model: {model}, input: \"{}\"",
                truncate(&input, EXCERPT_MAX_CHARS),
            )
        }
    }
}

/// Extracts the input text from an embedding request body based on
/// the provider's wire format.
fn extract_embed_input(json: &Value, provider: ProviderKind) -> String {
    match provider {
        // Both `Ollama` and `OpenAI` serialise embed input as a plain string.
        ProviderKind::Ollama | ProviderKind::OpenAi => {
            json["input"].as_str().unwrap_or("<nil>").to_owned()
        }
        // `Anthropic` has no embedding service; the platform gateway is not
        // mocked in this harness.
        ProviderKind::Anthropic | ProviderKind::Platform => "<nil>".to_owned(),
    }
}
