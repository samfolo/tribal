use tribal_db::{EmbeddingIndexRepository, EmbeddingTable, IndexState, PgEmbeddingIndexRepository};
use tribal_domain::EmbeddingProfileId;
use tribal_test_utils::TestDb;

/// The three-way catalogue check that makes a partial HNSW index build
/// idempotent and crash-safe, against a live database: absent builds, valid
/// skips, and an invalid (crashed) build is dropped and rebuilt.
#[tokio::test]
async fn test_ensure_partial_hnsw_three_way_check() {
    let ctx = TestDb::new().await;
    // CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction, so use a
    // committed raw connection rather than the rollback test transaction.
    let mut conn = ctx.raw_connection().await.expect("raw connection");
    let repo = PgEmbeddingIndexRepository;
    let table = EmbeddingTable::Embeddings;
    let epoch = 9_000_017_i64; // unique, clear of the genesis and reindex epochs
    let index = table.hnsw_index_name(epoch);
    let profile = EmbeddingProfileId::new();

    // Absent -> build -> valid.
    assert_eq!(
        repo.index_state(&mut conn, &index).await.expect("state"),
        IndexState::Absent,
    );
    repo.ensure_partial_hnsw(&mut conn, table, epoch, 768, profile)
        .await
        .expect("build");
    assert_eq!(
        repo.index_state(&mut conn, &index).await.expect("state"),
        IndexState::Valid,
    );

    // Valid -> a second call skips the rebuild.
    repo.ensure_partial_hnsw(&mut conn, table, epoch, 768, profile)
        .await
        .expect("re-ensure");
    assert_eq!(
        repo.index_state(&mut conn, &index).await.expect("state"),
        IndexState::Valid,
    );

    // Invalid (a crashed build) -> dropped and rebuilt. Marking the catalogue
    // entry invalid is the only way to produce deterministically the state a
    // failed CONCURRENTLY build leaves behind.
    sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::regclass")
        .bind(&index)
        .execute(&mut conn)
        .await
        .expect("mark the index invalid");
    assert_eq!(
        repo.index_state(&mut conn, &index).await.expect("state"),
        IndexState::Invalid,
    );
    repo.ensure_partial_hnsw(&mut conn, table, epoch, 768, profile)
        .await
        .expect("rebuild");
    assert_eq!(
        repo.index_state(&mut conn, &index).await.expect("state"),
        IndexState::Valid,
    );

    // Clean up so a re-run against the same database starts absent again.
    repo.drop_partial_hnsw(&mut conn, table, epoch)
        .await
        .expect("drop");
    assert_eq!(
        repo.index_state(&mut conn, &index).await.expect("state"),
        IndexState::Absent,
    );
}
