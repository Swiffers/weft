//! Layer-3 test of the door passing a caller on (`weft_dispatcher::proxy`)
//! against a REAL Postgres: a request held on its upstream (a route whose
//! run waits on a person) holds nothing that keeps the database listened
//! to, from before it waits on the upstream.
#![cfg(feature = "db-tests")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ConnectInfo;
use axum::routing::get;
use sqlx::PgPool;
use tower::ServiceExt as _;
use weft_task_store::pg_signal::{keep_listening, Listening, PgSignalWatch};

#[sqlx::test]
async fn a_request_waiting_on_its_upstream_holds_the_watch_no_longer(pool: PgPool) {
    static CHANNELS: &[&str] = &["weft_test_proxy"];
    let watch = PgSignalWatch::start(&pool.connect_options(), CHANNELS, Listening::WhileBusy).await.unwrap();
    // An upstream that answers once released, as a held route does.
    let (reached, release) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
    let upstream = axum::Router::new().route(
        "/held",
        get({
            let (reached, release) = (reached.clone(), release.clone());
            move || {
                let (reached, release) = (reached.clone(), release.clone());
                async move {
                    reached.notify_one();
                    release.notified().await;
                    "answered"
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

    let http = reqwest::Client::new();
    let door = axum::Router::new()
        .route(
            "/held",
            get(move |request: axum::extract::Request| {
                let http = http.clone();
                async move {
                    let upstream = weft_dispatcher::proxy::Upstream { what: "the upstream", base_url: format!("http://{address}"), auth: None, hold: None };
                    weft_dispatcher::proxy::forward(&http, upstream, "/held".into(), request, weft_dispatcher::proxy::Forwarding::Proxied).await
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(watch.clone(), keep_listening));
    let mut request = axum::http::Request::get("/held").header("host", "door").body(axum::body::Body::empty()).unwrap();
    request.extensions_mut().insert(ConnectInfo::<SocketAddr>("127.0.0.1:1".parse().unwrap()));
    let call = tokio::spawn(door.oneshot(request));
    tokio::time::timeout(Duration::from_secs(10), reached.notified()).await.expect("the upstream was reached");
    assert!(!watch.holds(), "nothing holds the watch while the upstream answers");
    release.notify_one();
    let answer = call.await.unwrap().unwrap();
    assert_eq!(axum::body::to_bytes(answer.into_body(), usize::MAX).await.unwrap(), "answered");
}
