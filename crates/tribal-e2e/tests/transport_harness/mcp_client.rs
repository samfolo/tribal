//! MCP test client and scope-based assertion helpers.

use std::{net::SocketAddr, time::Duration};

use tribal_domain::{Scope, is_authorised};
use tribal_mcp::tool_scope_registry;

use super::transport::test_client;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Timeout for reading an MCP response body.
///
/// Well above any realistic handler latency so a passing test never
/// hits this, but low enough that a broken test fails fast.
const MCP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// McpTestClient
// ---------------------------------------------------------------------------

/// Lightweight MCP-over-HTTP client for transport integration tests.
///
/// Manages the bearer token, session ID, and JSON-RPC message IDs
/// internally so test code focuses on assertions rather than protocol
/// plumbing.
pub struct McpTestClient {
    client: reqwest::Client,
    addr: SocketAddr,
    token: String,
    session_id: Option<String>,
    next_id: u64,
}

impl McpTestClient {
    pub fn new(addr: SocketAddr, token: &str) -> Self {
        Self {
            client: test_client(),
            addr,
            token: token.to_owned(),
            session_id: None,
            next_id: 1,
        }
    }

    /// Sends an MCP `initialize` request and stores the session ID.
    ///
    /// Reads chunks from the SSE response until the JSON-RPC result
    /// arrives, confirming the server has processed the request before
    /// subsequent calls.  Then sends the required `initialized`
    /// notification so the server accepts non-lifecycle requests.
    ///
    /// # Panics
    ///
    /// Panics if the server does not return 200, omits the
    /// `mcp-session-id` header, or does not produce an initialize
    /// result within the timeout.
    pub async fn initialise(&mut self) {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "initialize",
            "id": self.next_id(),
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "0.1" },
            },
        });

        let mut response = self.post(&body).await;

        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "initialize must return 200",
        );

        let session_id = response
            .headers()
            .get("mcp-session-id")
            .expect("initialize response must include mcp-session-id")
            .to_str()
            .expect("session ID must be valid UTF-8")
            .to_owned();

        // Read until we get the initialize result, confirming the
        // server has processed the request.
        let _result = read_sse_jsonrpc_result(&mut response).await;

        self.session_id = Some(session_id);

        // The MCP protocol requires an `initialized` notification
        // before the server will accept non-lifecycle requests.
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        });
        let notify_response = self.post(&notification).await;
        assert!(
            notify_response.status().is_success(),
            "initialized notification must succeed (got {})",
            notify_response.status(),
        );
    }

    /// Sends an MCP `tools/list` request and returns the tool names
    /// visible to the authenticated principal.
    ///
    /// # Panics
    ///
    /// Panics if the session has not been initialised, the server does
    /// not return 200, or the response does not contain a valid
    /// `tools/list` result within the timeout.
    pub async fn list_tool_names(&mut self) -> Vec<String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": self.next_id(),
            "params": {},
        });

        let mut response = self.post(&body).await;

        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "tools/list must return 200",
        );

        let result = read_sse_jsonrpc_result(&mut response).await;

        result
            .pointer("/result/tools")
            .and_then(|t| t.as_array())
            .expect("tools/list result must contain a tools array")
            .iter()
            .filter_map(|t| t["name"].as_str().map(String::from))
            .collect()
    }

    /// Returns the session ID, if the client has been initialised.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Sends a GET request to the MCP endpoint.
    ///
    /// Used to open a standalone SSE event stream for an existing
    /// session (the SSE reconnect path).
    ///
    /// # Panics
    ///
    /// Panics if the HTTP request fails at the transport level.
    pub async fn get(&self) -> reqwest::Response {
        let mut request = self
            .client
            .get(format!("http://{}/mcp", self.addr))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "text/event-stream");

        if let Some(session_id) = &self.session_id {
            request = request.header("Mcp-Session-Id", session_id);
        }

        request.send().await.expect("MCP GET request must succeed")
    }

    /// Sends a GET request with a `Last-Event-Id` header for SSE
    /// stream resumption.
    ///
    /// # Panics
    ///
    /// Panics if the session has not been initialised or the HTTP
    /// request fails.
    pub async fn get_with_last_event_id(&self, last_event_id: &str) -> reqwest::Response {
        let session_id = self
            .session_id
            .as_deref()
            .expect("session must be initialised before GET with Last-Event-Id");

        self.client
            .get(format!("http://{}/mcp", self.addr))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "text/event-stream")
            .header("Mcp-Session-Id", session_id)
            .header("Last-Event-Id", last_event_id)
            .send()
            .await
            .expect("MCP GET request must succeed")
    }

    async fn post(&self, body: &serde_json::Value) -> reqwest::Response {
        let mut request = self
            .client
            .post(format!("http://{}/mcp", self.addr))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "text/event-stream, application/json");

        if let Some(session_id) = &self.session_id {
            request = request.header("Mcp-Session-Id", session_id);
        }

        request
            .body(body.to_string())
            .send()
            .await
            .expect("MCP request must succeed")
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// Asserts that the tools visible to a principal match exactly those
/// whose required scope is satisfied by the granted scopes.
///
/// Calls `tools/list`, computes the expected set from the tool
/// registry, and asserts equality.
pub async fn assert_tool_visibility(mcp: &mut McpTestClient, granted_scopes: &[Scope]) {
    let mut actual = mcp.list_tool_names().await;
    actual.sort();

    let mut expected: Vec<String> = tool_scope_registry()
        .iter()
        .filter(|(_, required)| is_authorised(granted_scopes, required))
        .map(|(name, _)| (*name).to_owned())
        .collect();
    expected.sort();

    assert_eq!(
        actual, expected,
        "tools/list should reflect the principal's scopes",
    );
}

// ---------------------------------------------------------------------------
// SSE response parsing
// ---------------------------------------------------------------------------

/// Reads chunks from an SSE response until a `data:` line contains a
/// JSON-RPC object with a `result` field.
///
/// SSE streams in stateful mode stay open after sending the result
/// (for potential server notifications), so `response.text()` would
/// block forever.  This function reads incrementally and returns as
/// soon as the result event arrives.
///
/// # Panics
///
/// Panics if the stream ends or the timeout elapses before a
/// JSON-RPC result is found.
async fn read_sse_jsonrpc_result(response: &mut reqwest::Response) -> serde_json::Value {
    let mut buf = String::new();
    let deadline = tokio::time::Instant::now() + MCP_RESPONSE_TIMEOUT;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timeout waiting for JSON-RPC result, body so far:\n{buf}"
        );

        let chunk = tokio::time::timeout(remaining, response.chunk()).await;

        match chunk {
            Ok(Ok(Some(bytes))) => {
                buf.push_str(&String::from_utf8_lossy(&bytes));

                for line in buf.lines() {
                    let Some(json_str) = line.strip_prefix("data:") else {
                        continue;
                    };
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str.trim())
                    else {
                        continue;
                    };
                    if value.get("result").is_some() {
                        return value;
                    }
                    assert!(
                        value.get("error").is_none(),
                        "server returned JSON-RPC error: {}",
                        serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string()),
                    );
                }
            }
            Ok(Ok(None)) => {
                panic!("SSE stream ended without JSON-RPC result, body:\n{buf}");
            }
            Ok(Err(err)) => {
                panic!("SSE stream read error: {err}");
            }
            Err(elapsed) => {
                panic!("timeout waiting for JSON-RPC result ({elapsed}), body so far:\n{buf}");
            }
        }
    }
}
