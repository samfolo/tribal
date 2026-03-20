use serde_json::json;

use super::{
    server::TestHarness,
    tool_call::{tool_result_json, tool_result_text},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default total wait time for `expect_condition!` in milliseconds.
pub const DEFAULT_WAIT_MS: u64 = 10_000;

/// Default interval between poll iterations in milliseconds.
pub const DEFAULT_INTERVAL_MS: u64 = 200;

/// Maximum number of poll iterations for `expect_completion`.
const MAX_POLL_ITERATIONS: u64 = 60;

/// Seconds passed to `tribal_job_status` for long-polling.
const JOB_STATUS_WAIT_SECONDS: u64 = 2;

/// The only job outcome that `expect_completion` treats as success.
const EXPECTED_OUTCOME: &str = "success";

// ---------------------------------------------------------------------------
// expect_completion
// ---------------------------------------------------------------------------

/// Polls `tribal_job_status` until the job reaches a terminal state.
///
/// Returns normally only when the job completes with the `success` outcome.
/// A `partial` or `empty` outcome (some tasks dead-lettered or no items
/// produced) is treated as a failure — tests that expect those outcomes
/// should poll manually rather than using this helper.
///
/// # Panics
///
/// Panics with rich diagnostic context if the job does not complete with
/// a `success` outcome within the poll limit.
pub async fn expect_completion(harness: &TestHarness, job_id: &str) {
    let mut status = String::new();
    let mut outcome = String::new();

    for _ in 0..MAX_POLL_ITERATIONS {
        let result = harness
            .call_tool(
                "tribal_job_status",
                json!({ "job_id": job_id, "wait_seconds": JOB_STATUS_WAIT_SECONDS }),
            )
            .await;

        assert!(
            !result.is_error.unwrap_or(false),
            "tribal_job_status returned an error: {}",
            tool_result_text(&result),
        );

        let json = tool_result_json(&result);
        status = json["status"]
            .as_str()
            .expect("status field in job_status response")
            .to_owned();
        outcome = json["outcome"].as_str().unwrap_or("").to_owned();

        if status == "completed" || status == "failed" {
            break;
        }
    }

    if status == "completed" && outcome == EXPECTED_OUTCOME {
        return;
    }

    // Build diagnostic context and panic.
    let ctx = harness.diagnostic_context();
    let diagnostic = ctx.format_failure(job_id, &status, &outcome).await;
    panic!("{diagnostic}");
}

// ---------------------------------------------------------------------------
// expect_condition!
// ---------------------------------------------------------------------------

/// Polls a condition block until it returns `true` or the timeout elapses.
///
/// The body is a code block re-evaluated each iteration — no closures,
/// no borrow gymnastics. Whatever is in scope is used directly.
///
/// # Forms
///
/// ```ignore
/// expect_condition!("description", { body });
/// expect_condition!("description", wait: 30_000, { body });
/// expect_condition!("description", wait: 5_000, interval: 100, { body });
/// ```
macro_rules! expect_condition {
    ($desc:expr, wait: $wait:expr, interval: $interval:expr, $body:block) => {{
        let start = ::std::time::Instant::now();
        let wait_ms: u64 = $wait;
        let interval_ms: u64 = $interval;
        loop {
            let result: bool = $body;
            if result {
                break;
            }
            let elapsed = start.elapsed().as_millis() as u64;
            if elapsed >= wait_ms {
                panic!(
                    "Condition not met: '{}' (waited {}ms, timeout {}ms)",
                    $desc, elapsed, wait_ms,
                );
            }
            ::tokio::time::sleep(::std::time::Duration::from_millis(interval_ms)).await;
        }
    }};
    ($desc:expr, wait: $wait:expr, $body:block) => {
        $crate::harness::polling::expect_condition!(
            $desc,
            wait: $wait,
            interval: $crate::harness::polling::DEFAULT_INTERVAL_MS,
            $body
        )
    };
    ($desc:expr, $body:block) => {
        $crate::harness::polling::expect_condition!(
            $desc,
            wait: $crate::harness::polling::DEFAULT_WAIT_MS,
            interval: $crate::harness::polling::DEFAULT_INTERVAL_MS,
            $body
        )
    };
}

pub(crate) use expect_condition;
