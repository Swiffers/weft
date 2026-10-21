//! Layer-3 contract test for a program's instance token mint
//! (`program_tokens::store_instance_token`) against a REAL Postgres: the
//! replace-on-replay and the guard that keeps one instance's token id
//! from being taken over live in one SQL statement.

use sqlx::PgPool;
use uuid::Uuid;

use weft_broker::program_tokens::{store_instance_token, InstanceTokenRow};
use weft_core::instance::InstanceId;
use weft_dispatcher::store::postgres::PostgresStore;
use weft_dispatcher::store::TokenStore;

const TENANT: &str = "t1";

fn row<'a>(project_id: Uuid, instance: &'a InstanceId) -> InstanceTokenRow<'a> {
    InstanceTokenRow {
        id: Uuid::from_u128(7),
        tenant: TENANT,
        project_id,
        instance,
        name: None,
        displays: true,
        created_at: 1,
        expires_at: 100,
    }
}

/// A mint under an id that already names the instance's token replaces
/// that token (the old value stops working), and an id naming any other
/// token writes nothing.
#[sqlx::test]
async fn a_replayed_program_mint_replaces_its_token(pool: PgPool) {
    weft_task_store::apply_groups(&pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let store = PostgresStore::from_pool(pool.clone());
    let project = Uuid::new_v4();
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let hash = weft_core::signal_token::token_hash;

    assert!(store_instance_token(&pool, &row(project, &ada), "first-value").await.unwrap());
    assert!(store_instance_token(&pool, &row(project, &ada), "second-value").await.unwrap(), "the replay replaces");
    assert!(store.get_signal_token(&hash("first-value")).await.unwrap().is_none(), "the old value is dead");
    let live = store.get_signal_token(&hash("second-value")).await.unwrap().expect("the new value works");
    assert_eq!((live.id, live.instance.as_ref(), live.all_displays), (Uuid::from_u128(7), Some(&ada), true));

    assert!(!store_instance_token(&pool, &row(project, &bob), "stolen").await.unwrap(), "bob cannot take ada's id");
    assert!(!store_instance_token(&pool, &row(Uuid::new_v4(), &ada), "moved").await.unwrap(), "nor another project");
    assert!(store.get_signal_token(&hash("stolen")).await.unwrap().is_none());
    assert!(store.get_signal_token(&hash("moved")).await.unwrap().is_none());
}
