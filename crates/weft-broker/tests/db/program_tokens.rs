//! Layer-3 contract test for the instance token mint
//! (`weft_access_store::instance_tokens::mint`, which a program's mint
//! through the broker and a person's `weft token mint --instance` both go
//! through) against a REAL Postgres: the replace-on-replay, the guard that
//! keeps one instance's token id from being taken over, and the project
//! held to the owner live in one SQL statement.

use sqlx::PgPool;
use uuid::Uuid;

use weft_access_store::instance_tokens::{mint, InstanceTokenMint, MintRefused};
use weft_core::instance::InstanceId;
use weft_dispatcher::store::postgres::PostgresStore;
use weft_dispatcher::store::TokenStore;

const TENANT: &str = "t1";

async fn project(pool: &PgPool, tenant: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at, project_json_version, resync_version, running_build_inputs_version) \
         VALUES ($1, 'p', $2, 'registered', '{}', 0, $3, 1, 1)",
    )
    .bind(id)
    .bind(tenant)
    .bind(weft_core::ProjectDefinition::STORED.version())
    .execute(pool)
    .await
    .expect("project row");
    id
}

fn asked<'a>(project: Uuid, instance: &'a InstanceId) -> InstanceTokenMint<'a> {
    InstanceTokenMint {
        id: Uuid::from_u128(7),
        tenant: TENANT,
        project,
        instance,
        name: None,
        allowed_tags: &[],
        allowed_displays: &[],
        all_displays: true,
        expires_in_secs: 100,
        now_unix: 1,
    }
}

/// A mint under an id that already names the instance's token replaces
/// that token (the old value stops working), an id naming any other token
/// writes nothing, and a project of another owner is refused as one that
/// does not exist.
#[sqlx::test]
async fn a_replayed_mint_replaces_its_token_and_holds_the_project_to_the_owner(pool: PgPool) {
    weft_task_store::apply_groups(&pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let store = PostgresStore::from_pool(pool.clone());
    let own = project(&pool, TENANT).await;
    let sibling = project(&pool, TENANT).await;
    let foreign = project(&pool, "t2").await;
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let hash = weft_core::signal_token::token_hash;

    let first = mint(&pool, &asked(own, &ada)).await.unwrap();
    assert_eq!(first.expires_at, 101);
    let second = mint(&pool, &asked(own, &ada)).await.unwrap();
    assert!(store.get_signal_token(&hash(&first.token)).await.unwrap().is_none(), "the old value is dead");
    let live = store.get_signal_token(&hash(&second.token)).await.unwrap().expect("the new value works");
    assert_eq!((live.id, live.instance.as_ref(), live.all_displays), (Uuid::from_u128(7), Some(&ada), true));
    assert_eq!(live.allowed_projects, vec![own]);

    assert!(matches!(mint(&pool, &asked(own, &bob)).await, Err(MintRefused::IdTaken(_))), "bob cannot take ada's id");
    assert!(matches!(mint(&pool, &asked(sibling, &ada)).await, Err(MintRefused::IdTaken(_))), "nor another project");
    assert!(matches!(mint(&pool, &asked(foreign, &ada)).await, Err(MintRefused::NoProject)), "another owner's project");
    assert!(matches!(mint(&pool, &asked(Uuid::new_v4(), &ada)).await, Err(MintRefused::NoProject)), "a project that is not there");
    let elsewhere = InstanceTokenMint { id: Uuid::from_u128(8), ..asked(sibling, &ada) };
    let minted = mint(&pool, &elsewhere).await.expect("a sibling project of the same owner");
    let row = store.get_signal_token(&hash(&minted.token)).await.unwrap().expect("stored");
    assert_eq!(row.allowed_projects, vec![sibling]);
    assert!(matches!(
        mint(&pool, &InstanceTokenMint { id: Uuid::from_u128(9), expires_in_secs: 0, ..asked(own, &ada) }).await,
        Err(MintRefused::Expiry(_))
    ));
    assert_eq!(store.list_signal_tokens(TENANT).await.unwrap().len(), 2);
}
