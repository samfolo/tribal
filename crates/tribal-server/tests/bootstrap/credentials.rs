//! Tests for the credentials.json write site, exercised by `token create`
//! (the lightest writer; bootstrap and setup share the same atomic-rename
//! contract).

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tribal_common::sha256_hex;
use tribal_config::{
    Auth, CREDENTIALS_WRITE_FAILED_PREFIX, CliOverrides, Credentials, TransportKind,
};
use tribal_db::{AuthTokenRepository, PgAuthTokenRepository};
use tribal_test_utils::{serial_lock, test_context};

use super::common::{TestEnv, fresh_db, run_bootstrap, run_setup, run_token_create};

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

/// Successful token-create must leave credentials.json at the resolved
/// path with POSIX mode `0600` and no tempfile residue.
#[cfg(unix)]
#[tokio::test]
async fn test_credentials_written_with_locked_mode_no_residue() {
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
async fn test_credentials_unwritable_parent_emits_warning_and_keeps_token_row() {
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
// Standalone setup exercises the credentials write site too
// ---------------------------------------------------------------------------

/// `tribal setup` must persist credentials.json on its own. The file
/// content must match the freshly-minted bearer.
#[tokio::test]
async fn test_setup_writes_credentials_with_minted_bearer() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let (outcome, _stderr) = run_setup(ctx, &env.config_path, CliOverrides::default(), None)
        .await
        .expect("setup succeeds");

    let creds_path = env.credentials_path();
    assert!(
        creds_path.exists(),
        "credentials.json present at {creds_path:?}",
    );
    let raw = std::fs::read_to_string(&creds_path).expect("read credentials.json");
    let parsed: Credentials = serde_json::from_str(&raw).expect("parse json");
    let Auth::Bearer { token: persisted } = parsed.auth;
    assert_eq!(
        persisted, outcome.bearer_token,
        "persisted token matches the minted bearer",
    );
}

/// A read-only credentials parent must not abort `tribal setup`: the
/// warn-and-success literal lands on stderr, the DB token row is still
/// valid, and the function returns Ok.
#[cfg(unix)]
#[tokio::test]
async fn test_setup_credentials_unwritable_emits_warning_and_keeps_token_row() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let tribal_dir = env.xdg_dir.path().join("tribal");
    std::fs::create_dir(&tribal_dir).expect("pre-create tribal dir");
    std::fs::set_permissions(&tribal_dir, std::fs::Permissions::from_mode(0o500))
        .expect("chmod r-x");

    let (outcome, stderr) = run_setup(ctx, &env.config_path, CliOverrides::default(), None)
        .await
        .expect("setup still succeeds when credentials write fails");

    let stderr = String::from_utf8(stderr).expect("utf8 stderr");
    assert!(
        stderr.contains(CREDENTIALS_WRITE_FAILED_PREFIX),
        "expected canonical warn-and-success prefix in: {stderr}",
    );

    // DB row must be present and valid even though credentials.json failed.
    let mut conn = pool.acquire().await.expect("acquire");
    let token_hash = sha256_hex(outcome.bearer_token.as_str());
    let row = PgAuthTokenRepository
        .find_by_hash(&mut conn, &token_hash)
        .await
        .expect("query token");
    assert!(row.is_some(), "minted token row must exist in DB");

    // Restore so the tempdir cleanup can run.
    std::fs::set_permissions(&tribal_dir, std::fs::Permissions::from_mode(0o700)).ok();
}

// ---------------------------------------------------------------------------
// Bootstrap: credentials failure surfaces the warning alongside the hand-off
// ---------------------------------------------------------------------------

/// A read-only credentials parent must not abort bootstrap: the
/// hand-off still renders the freshly-minted token, and the warn-and-
/// success literal lands on stderr alongside it. Locks the warning's
/// position relative to the polished output so a regression that
/// drops the emit (or moves it) is caught.
#[cfg(unix)]
#[tokio::test]
async fn test_bootstrap_credentials_unwritable_emits_warning_with_handoff() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let tribal_dir = env.xdg_dir.path().join("tribal");
    std::fs::create_dir(&tribal_dir).expect("pre-create tribal dir");
    std::fs::set_permissions(&tribal_dir, std::fs::Permissions::from_mode(0o500))
        .expect("chmod r-x");

    let (_stdout, stderr) = run_bootstrap(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        None,
        TransportKind::Stdio,
        false,
    )
    .await
    .expect("bootstrap still succeeds when credentials write fails");

    let stderr = String::from_utf8(stderr).expect("utf8 stderr");
    let handoff_offset = stderr.find("Bootstrap complete.").unwrap_or_else(|| {
        panic!("hand-off must still render alongside the warning: {stderr}");
    });
    let warning_count = stderr.matches(CREDENTIALS_WRITE_FAILED_PREFIX).count();
    assert_eq!(
        warning_count, 1,
        "warn-and-success literal must appear exactly once: {stderr}",
    );
    let warning_offset = stderr
        .find(CREDENTIALS_WRITE_FAILED_PREFIX)
        .expect("warning prefix present, asserted above");
    assert!(
        warning_offset > handoff_offset,
        "warn-and-success literal must follow the hand-off block; \
         got warning at {warning_offset}, hand-off at {handoff_offset}: {stderr}",
    );

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
async fn test_credentials_token_create_independent_of_setup() {
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
async fn test_credentials_concurrent_writes_last_writer_wins() {
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
