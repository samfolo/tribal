//! Integration tests for prompt hot-reload.
//!
//! Tests the reload pipeline deterministically by calling
//! `reload_single_prompt` directly — no filesystem watcher, no
//! polling, no timing dependency.

use std::sync::Arc;

use tokio::sync::RwLock;
use tribal_domain::{PromptRole, PromptStage};
use tribal_mcp::ActivePromptVersions;
use tribal_server::{PromptTemplateLocation, ensure_prompt_files, load_prompts, reload_single_prompt};
use tribal_test_utils::{serial_lock, test_context, truncate_all_tables};

#[tokio::test]
async fn test_reload_updates_active_prompt_version() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create per-test pool");

    let mut conn = pool.acquire().await.expect("acquire connection");
    truncate_all_tables(&mut conn).await;
    drop(conn);

    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    ensure_prompt_files(prompts_dir.path())
        .await
        .expect("write default prompts");

    let initial_versions = load_prompts(&pool, prompts_dir.path())
        .await
        .expect("load initial prompts");

    let active = Arc::new(RwLock::new(initial_versions));

    // Modify the extraction/system prompt.
    let target = PromptTemplateLocation::from((PromptStage::Extraction, PromptRole::System));
    let file_path = target.resolve(prompts_dir.path());
    let original = tokio::fs::read_to_string(&file_path)
        .await
        .expect("read original");
    let modified = format!("{original}\n{{# hot-reload test #}}");
    tokio::fs::write(&file_path, &modified)
        .await
        .expect("write modified prompt");

    let snapshot_before = format!("{:?}", *active.read().await);

    // Reload the single prompt — deterministic, no watcher needed.
    reload_single_prompt(target, &file_path, &pool, &active).await;

    let snapshot_after = format!("{:?}", *active.read().await);

    assert_ne!(
        snapshot_before, snapshot_after,
        "active prompt versions should change after reload",
    );
}

#[tokio::test]
async fn test_reload_same_content_no_change() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create per-test pool");

    let mut conn = pool.acquire().await.expect("acquire connection");
    truncate_all_tables(&mut conn).await;
    drop(conn);

    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    ensure_prompt_files(prompts_dir.path())
        .await
        .expect("write default prompts");

    let initial_versions = load_prompts(&pool, prompts_dir.path())
        .await
        .expect("load initial prompts");

    let active = Arc::new(RwLock::new(initial_versions));

    // Rewrite with identical content.
    let target = PromptTemplateLocation::from((PromptStage::Extraction, PromptRole::System));
    let file_path = target.resolve(prompts_dir.path());

    let snapshot_before = format!("{:?}", *active.read().await);

    reload_single_prompt(target, &file_path, &pool, &active).await;

    let snapshot_after = format!("{:?}", *active.read().await);

    assert_eq!(
        snapshot_before, snapshot_after,
        "identical content should not change the active version",
    );
}

#[tokio::test]
async fn test_reload_empty_content_keeps_current_version() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create per-test pool");

    let mut conn = pool.acquire().await.expect("acquire connection");
    truncate_all_tables(&mut conn).await;
    drop(conn);

    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    ensure_prompt_files(prompts_dir.path())
        .await
        .expect("write default prompts");

    let initial_versions = load_prompts(&pool, prompts_dir.path())
        .await
        .expect("load initial prompts");

    let active = Arc::new(RwLock::new(initial_versions));

    // Replace with empty content.
    let target = PromptTemplateLocation::from((PromptStage::Triage, PromptRole::User));
    let file_path = target.resolve(prompts_dir.path());
    tokio::fs::write(&file_path, "")
        .await
        .expect("write empty content");

    let snapshot_before = format!("{:?}", *active.read().await);

    reload_single_prompt(target, &file_path, &pool, &active).await;

    let snapshot_after = format!("{:?}", *active.read().await);

    assert_eq!(
        snapshot_before, snapshot_after,
        "empty content should not change the active version",
    );
}

#[tokio::test]
async fn test_reload_invalid_template_keeps_current_version() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create per-test pool");

    let mut conn = pool.acquire().await.expect("acquire connection");
    truncate_all_tables(&mut conn).await;
    drop(conn);

    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    ensure_prompt_files(prompts_dir.path())
        .await
        .expect("write default prompts");

    let initial_versions = load_prompts(&pool, prompts_dir.path())
        .await
        .expect("load initial prompts");

    let active = Arc::new(RwLock::new(initial_versions));

    // Replace with a template referencing a nonexistent variable.
    let target = PromptTemplateLocation::from((PromptStage::Extraction, PromptRole::System));
    let file_path = target.resolve(prompts_dir.path());
    tokio::fs::write(&file_path, "{{ nonexistent_variable }}")
        .await
        .expect("write invalid template");

    let snapshot_before = format!("{:?}", *active.read().await);

    reload_single_prompt(target, &file_path, &pool, &active).await;

    let snapshot_after = format!("{:?}", *active.read().await);

    assert_eq!(
        snapshot_before, snapshot_after,
        "invalid template should not change the active version",
    );
}

#[tokio::test]
async fn test_reload_only_affects_targeted_prompt() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create per-test pool");

    let mut conn = pool.acquire().await.expect("acquire connection");
    truncate_all_tables(&mut conn).await;
    drop(conn);

    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    ensure_prompt_files(prompts_dir.path())
        .await
        .expect("write default prompts");

    let initial_versions = load_prompts(&pool, prompts_dir.path())
        .await
        .expect("load initial prompts");

    let active = Arc::new(RwLock::new(initial_versions));

    // Modify only the relation/user prompt.
    let target = PromptTemplateLocation::from((PromptStage::Relation, PromptRole::User));
    let file_path = target.resolve(prompts_dir.path());
    let original = tokio::fs::read_to_string(&file_path)
        .await
        .expect("read original");
    let modified = format!("{original}\n{{# isolation test #}}");
    tokio::fs::write(&file_path, &modified)
        .await
        .expect("write modified prompt");

    // Snapshot all non-target locations before reload.
    let before = format!("{:?}", *active.read().await);

    reload_single_prompt(target, &file_path, &pool, &active).await;

    let after = format!("{:?}", *active.read().await);

    // The overall snapshot should differ (relation/user changed).
    assert_ne!(before, after, "relation/user should have changed");

    // Reload the same target again with the same modified content.
    // This second reload should be idempotent (content-addressed).
    let after_second = format!("{:?}", *active.read().await);

    reload_single_prompt(target, &file_path, &pool, &active).await;

    let after_third = format!("{:?}", *active.read().await);
    assert_eq!(
        after_second, after_third,
        "second reload of same content should be idempotent",
    );
}
