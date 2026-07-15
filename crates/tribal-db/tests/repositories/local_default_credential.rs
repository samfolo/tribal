use tribal_db::{
    AuthTokenRepository, LocalDefaultCredentialRepository, PgAuthTokenRepository,
    PgLocalDefaultCredentialRepository, PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::CredentialGenerationId;
use tribal_test_utils::{TestDb, a_new_auth_token, a_new_principal};

#[tokio::test]
async fn test_local_default_credentials_replace_and_delete_independently() {
    let database = TestDb::new().await;
    let mut transaction = database.begin().await.expect("begin");
    let principal = PgPrincipalRepository
        .insert(
            &mut transaction,
            &a_new_principal()
                .principal_key("principal:credential-mapping".to_owned())
                .build(),
        )
        .await
        .expect("principal inserts");
    let first_token = PgAuthTokenRepository
        .insert(
            &mut transaction,
            &a_new_auth_token()
                .principal_id(principal.id())
                .token_hash("1".repeat(64))
                .build(),
        )
        .await
        .expect("first token inserts");
    let second_token = PgAuthTokenRepository
        .insert(
            &mut transaction,
            &a_new_auth_token()
                .principal_id(principal.id())
                .token_hash("2".repeat(64))
                .build(),
        )
        .await
        .expect("second token inserts");
    let repository = PgLocalDefaultCredentialRepository;
    let first_namespace = "0123456789abcdef01234567";
    let second_namespace = "fedcba9876543210fedcba98";
    let first_generation = CredentialGenerationId::new();
    let second_generation = CredentialGenerationId::new();

    repository
        .replace(
            &mut transaction,
            first_namespace,
            first_generation,
            first_token.id(),
        )
        .await
        .expect("first mapping replaces");
    repository
        .replace(
            &mut transaction,
            second_namespace,
            second_generation,
            second_token.id(),
        )
        .await
        .expect("second mapping replaces");

    let first = repository
        .find(&mut transaction, first_namespace)
        .await
        .expect("first mapping reads")
        .expect("first mapping exists");
    let second = repository
        .find(&mut transaction, second_namespace)
        .await
        .expect("second mapping reads")
        .expect("second mapping exists");
    assert_eq!(first.generation_id, first_generation);
    assert_eq!(first.token_id, first_token.id());
    assert_eq!(second.generation_id, second_generation);
    assert_eq!(second.token_id, second_token.id());

    assert!(
        repository
            .delete(&mut transaction, first_namespace)
            .await
            .expect("first mapping deletes")
    );
    assert!(
        repository
            .find(&mut transaction, first_namespace)
            .await
            .expect("first mapping absence reads")
            .is_none()
    );
    assert!(
        repository
            .find(&mut transaction, second_namespace)
            .await
            .expect("second mapping remains readable")
            .is_some()
    );
}
