//! The managed-runtime acceptance walk against a live metering-gateway stack.
//!
//! The stack is launched out of process by `just e2e`, which hands its
//! coordinates in through `CORTEX_E2E_*`. Absent those — the ordinary `just test`
//! run has no stack — every test here skips, so the walk runs only under the
//! harness that stands its dependencies up.

use std::env;

/// Whether to skip for want of a running stack — the ordinary test run, which
/// exports no coordinates. Reported so a skipped walk is visible, not silent.
fn skip_without_stack() -> bool {
    if env::var("CORTEX_E2E_READY").is_err() {
        eprintln!("skipping the managed-runtime walk: no e2e stack (run `just e2e`)");
        return true;
    }
    false
}

/// Smoke the wiring: the launcher's gateway is reachable, the bearer it minted
/// authenticates, and the account it seeded and funded reads back its balance.
#[tokio::test]
async fn managed_stack_answers_a_funded_balance_read() {
    if skip_without_stack() {
        return;
    }
    let gateway = env::var("CORTEX_E2E_GATEWAY_URL").expect("the launcher exports the gateway url");
    let bearer = env::var("CORTEX_E2E_GATEWAY_BEARER").expect("the launcher exports the bearer");

    let response = reqwest::Client::new()
        .get(format!("{gateway}/v1/balance"))
        .bearer_auth(&bearer)
        .send()
        .await
        .expect("the balance read reaches the gateway");
    assert!(
        response.status().is_success(),
        "the gateway answers the balance read (status {})",
        response.status(),
    );

    let body: serde_json::Value = response.json().await.expect("the balance decodes");
    assert_eq!(
        body["settled_nanodollars"], 1_000_000_000_i64,
        "the funded balance is visible through the gateway",
    );
}
