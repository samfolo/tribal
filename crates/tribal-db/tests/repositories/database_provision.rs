use sqlx::Connection;
use tribal_db::{DatabaseCreation, DatabaseProvisionRepository, PgDatabaseProvisionRepository};
use tribal_test_utils::TestDb;

/// A generous bound for ordinary provisioning; the timeout only matters when
/// another session holds the provisioning lock.
const TIMEOUT_MS: u64 = 30_000;

#[tokio::test]
async fn test_create_database_if_absent_creates_then_reports_present() {
    let ctx = TestDb::new().await;
    let mut conn = ctx.raw_connection().await.expect("connect");
    let repo = PgDatabaseProvisionRepository;
    let name = format!("p_{}", uuid::Uuid::new_v4().simple());

    let first = repo
        .create_database_if_absent(&mut conn, &name, TIMEOUT_MS)
        .await
        .expect("first provision");
    let second = repo
        .create_database_if_absent(&mut conn, &name, TIMEOUT_MS)
        .await
        .expect("second provision");

    assert_eq!(first, DatabaseCreation::Created);
    assert_eq!(second, DatabaseCreation::AlreadyPresent);

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_database WHERE datname = $1")
        .bind(&name)
        .fetch_one(&mut conn)
        .await
        .expect("count databases");
    assert_eq!(count, 1);

    drop_database(&mut conn, &name).await;
}

#[tokio::test]
async fn test_racing_provisions_create_exactly_one_database() {
    let ctx = TestDb::new().await;
    let name = format!("p_{}", uuid::Uuid::new_v4().simple());

    let mut first_conn = ctx.raw_connection().await.expect("connect first");
    let mut second_conn = ctx.raw_connection().await.expect("connect second");
    let (first, second) = tokio::join!(
        PgDatabaseProvisionRepository.create_database_if_absent(&mut first_conn, &name, TIMEOUT_MS),
        PgDatabaseProvisionRepository.create_database_if_absent(
            &mut second_conn,
            &name,
            TIMEOUT_MS
        ),
    );
    let outcomes = [
        first.expect("first provision"),
        second.expect("second provision"),
    ];

    assert!(outcomes.contains(&DatabaseCreation::Created));
    assert!(outcomes.contains(&DatabaseCreation::AlreadyPresent));

    let mut conn = ctx.raw_connection().await.expect("connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_database WHERE datname = $1")
        .bind(&name)
        .fetch_one(&mut conn)
        .await
        .expect("count databases");
    assert_eq!(count, 1);

    drop_database(&mut conn, &name).await;
}

#[tokio::test]
async fn test_a_held_lock_fails_bounded_instead_of_hanging() {
    let ctx = TestDb::new().await;
    let name = format!("p_{}", uuid::Uuid::new_v4().simple());

    // Hold the provisioning lock from a separate session for the whole test.
    let mut holder = ctx.raw_connection().await.expect("connect holder");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(tribal_db::advisory_locks::DATABASE_PROVISION)
        .execute(&mut holder)
        .await
        .expect("hold the provisioning lock");

    let mut conn = ctx.raw_connection().await.expect("connect");
    let started = std::time::Instant::now();
    let refused = PgDatabaseProvisionRepository
        .create_database_if_absent(&mut conn, &name, 500)
        .await;

    assert!(refused.is_err(), "a held lock must not report an outcome");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the statement timeout must cancel the lock wait",
    );

    let _ = holder.close().await;
}

#[tokio::test]
async fn test_the_duplicate_backstop_decides_a_race_the_lock_never_saw() {
    // The lock is the arbiter; this proves the backstop that decides when a
    // racer beats it — two bare creates, no lock between them.
    let ctx = TestDb::new().await;
    let name = format!("p_{}", uuid::Uuid::new_v4().simple());
    let mut first = ctx.raw_connection().await.expect("connect first");
    let mut second = ctx.raw_connection().await.expect("connect second");

    let (a, b) = tokio::join!(
        bare_create(&mut first, &name),
        bare_create(&mut second, &name)
    );
    let outcomes = [a, b];

    assert_eq!(
        outcomes.iter().filter(|created| **created).count(),
        1,
        "exactly one bare create wins the race"
    );
    assert_eq!(
        outcomes.iter().filter(|created| !**created).count(),
        1,
        "the loser reports the duplicate-database SQLSTATE, not an error"
    );

    let mut conn = ctx.raw_connection().await.expect("connect");
    drop_database(&mut conn, &name).await;
}

/// Creates without the provisioning lock; `false` means the duplicate
/// SQLSTATE claimed it.
async fn bare_create(conn: &mut sqlx::PgConnection, name: &str) -> bool {
    use sqlx::Executor as _;
    match conn
        .execute(format!("CREATE DATABASE \"{name}\"").as_str())
        .await
    {
        Ok(_) => true,
        Err(error) => {
            let code = error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .map(|code| code.to_string());
            // A racing create loses on pg_database's name index, so the
            // loser is a unique violation rather than duplicate_database.
            assert_eq!(
                code.as_deref(),
                Some("23505"),
                "a genuine race reports the unique violation the backstop must accept"
            );
            false
        }
    }
}

async fn drop_database(conn: &mut sqlx::PgConnection, name: &str) {
    use sqlx::Executor as _;
    let _ = conn
        .execute(format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)").as_str())
        .await;
}
