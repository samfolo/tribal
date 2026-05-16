//! Tests for the credentials.json write site, exercised by `token create`
//! (the lightest writer; bootstrap and setup share the same atomic-rename
//! contract).

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tribal_common::sha256_hex;
use tribal_config::{Auth, CREDENTIALS_WRITE_FAILED_PREFIX, Credentials};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository};
use tribal_test_utils::{serial_lock, test_context};

use super::common::{TestEnv, fresh_db, run_token_create};

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

/// Successful token-create must leave credentials.json at the resolved
/// path with POSIX mode `0600` and no tempfile residue.
#[cfg(unix)]
#[tokio::test]
async fn credentials_written_with_locked_mode_no_residue() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let (_token, _stderr) = run_token_create(ctx, &env.config_path, None)
        .await
        .expect("token-create succeeds");

    let creds_path = env.credentials_path();
    assert!(
        creds_path.exists(),
        "credentials.json present at {creds_path:?}"
    );

    let mode = std::fs::metadata(&creds_path)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "credentials.json mode must be 0600");

    let parent = creds_path.parent().expect("parent dir");
    let leftovers: Vec<_> = std::fs::read_dir(parent)
        .expect("read parent")
        .filter_map(Result::ok)
        .filter(|e| e.file_name() != "credentials.json")
        .collect();
    assert!(
        leftovers.is_empty(),
        "tempfile residue in {parent:?}: {leftovers:?}",
    );
}

// ---------------------------------------------------------------------------
// Read-only parent: warn-and-success
// ---------------------------------------------------------------------------

/// A read-only parent directory must surface the warn-and-success
/// literal, leave the DB row valid, and still return success.
#[cfg(unix)]
#[tokio::test]
async fn credentials_unwritable_parent_emits_warning_and_keeps_token_row() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let tribal_dir = env.xdg_dir.path().join("tribal");
    std::fs::create_dir(&tribal_dir).expect("pre-create tribal dir");
    std::fs::set_permissions(&tribal_dir, std::fs::Permissions::from_mode(0o500))
        .expect("chmod r-x");

    let (token, stderr) = run_token_create(ctx, &env.config_path, None)
        .await
        .expect("token-create still succeeds");

    let stderr = String::from_utf8(stderr).expect("utf8 stderr");
    assert!(
        stderr.contains(CREDENTIALS_WRITE_FAILED_PREFIX),
        "expected canonical warn-and-success prefix in: {stderr}",
    );

    // DB row should be present and valid even though the file write failed.
    let mut conn = pool.acquire().await.expect("acquire");
    let token_hash = sha256_hex(token.as_str());
    let row = PgAuthTokenRepository
        .find_by_hash(&mut conn, &token_hash)
        .await
        .expect("query token");
    assert!(row.is_some(), "minted token row must exist in DB");

    // Restore so the tempdir cleanup can run.
    std::fs::set_permissions(&tribal_dir, std::fs::Permissions::from_mode(0o700)).ok();
}

// ---------------------------------------------------------------------------
// token-create write site exercised independently
// ---------------------------------------------------------------------------

/// `tribal token create` must write credentials.json on its own (no
/// setup or bootstrap prerequisite). Verifies the file contents match
/// the freshly-minted bearer.
#[tokio::test]
async fn credentials_token_create_independent_of_setup() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let (token, _stderr) = run_token_create(ctx, &env.config_path, None)
        .await
        .expect("token-create succeeds");

    let creds_path = env.credentials_path();
    let raw = std::fs::read_to_string(&creds_path).expect("read credentials.json");
    let parsed: Credentials = serde_json::from_str(&raw).expect("parse json");

    let Auth::Bearer { token: persisted } = parsed.auth;
    assert_eq!(
        persisted, token,
        "persisted token matches the minted bearer"
    );
}

// ---------------------------------------------------------------------------
// Concurrent writes: atomic rename keeps the file consistent
// ---------------------------------------------------------------------------

/// Two parallel `token create` invocations against the same
/// `XDG_CONFIG_HOME` must both succeed and leave a single
/// well-formed credentials.json whose token matches one of the
/// freshly-minted bearers (last-writer-wins).
#[tokio::test]
async fn credentials_concurrent_writes_last_writer_wins() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let config_path = env.config_path.clone();
    let (token_a, token_b) = tokio::join!(
        run_token_create(ctx, &config_path, None),
        run_token_create(ctx, &config_path, None),
    );
    let (token_a, _) = token_a.expect("first token-create succeeds");
    let (token_b, _) = token_b.expect("second token-create succeeds");
    assert_ne!(token_a, token_b, "two invocations mint distinct tokens");

    let creds_path = env.credentials_path();
    let raw = std::fs::read_to_string(&creds_path).expect("read credentials.json");
    let parsed: Credentials = serde_json::from_str(&raw).expect("parse json");
    let Auth::Bearer { token: persisted } = parsed.auth;
    assert!(
        persisted == token_a || persisted == token_b,
        "persisted token must match one of the two minted bearers",
    );
}
