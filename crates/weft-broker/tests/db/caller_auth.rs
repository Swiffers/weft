//! Layer-3 contract tests for `caller_auth::verify_caller`: the real
//! check against a REAL Postgres holding a stored connection, so the
//! whole chain (tenant wall, the recipe's `verify` block, the stored
//! material, the scheme) runs end to end. The identity-token scheme
//! needs an issuer's published keys over the network, so only what it
//! refuses before any fetch is covered here; the shared-key and HMAC
//! schemes are covered whole.

use serde_json::json;
use sqlx::PgPool;

use weft_access_store::connect_direct;
use weft_broker::caller_auth::{verify_caller, CallerRefusal, HeldVerifiers};
use weft_broker_client::protocol::CallerVerifyRequest;
use weft_core::access::spec::Door;
use weft_core::access::wire::ConnectDirect;
use weft_core::AccessSpec;

const TENANT: &str = "tenant-a";

async fn schema(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&weft_access_store::GROUP]).await.expect("schema");
}

/// A fresh held copy of the verifiers with nothing kept yet, so every
/// check reads its connection from the database.
fn verifiers() -> std::sync::Arc<HeldVerifiers> {
    HeldVerifiers::unfollowed(16, |_| true)
}

/// The shape of a shipped auth access node's recipe: pasted fields, no
/// test call (a verifier never dials out), a `verify` block.
fn auth_spec(service: &str, fields: &[&str], verify: serde_json::Value) -> AccessSpec {
    serde_json::from_value(json!({
        "service": service,
        "acquisition": {
            "kind": "static",
            "fields": fields.iter().map(|f| json!({ "name": f })).collect::<Vec<_>>()
        },
        "verify": verify
    }))
    .expect("a valid auth recipe")
}

async fn store(pool: &PgPool, tenant: &str, spec: AccessSpec, values: &[(&str, &str)]) -> String {
    let done = connect_direct(
        pool,
        tenant,
        ConnectDirect {
            paste: false,
            spec,
            door: Door::Own,
            registration: None,
            values: values.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            label: Some("gate".into()),
            permissions: vec![],
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect("a pasted verifier connection stores without a test call");
    done.grant.id.to_string()
}

fn request(
    tenant: &str,
    access_id: &str,
    service: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> CallerVerifyRequest {
    use base64::Engine as _;
    CallerVerifyRequest {
        tenant: tenant.into(),
        access_id: access_id.into(),
        for_instance: None,
        service: service.into(),
        method: "POST".into(),
        path: "chat/room7".into(),
        url: Some("https://acme.example/chat/room7".into()),
        headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        body_b64: base64::engine::general_purpose::STANDARD.encode(body),
    }
}

fn refused(r: Result<weft_broker_client::protocol::CallerVerified, CallerRefusal>) -> String {
    match r {
        Err(CallerRefusal::Refused(why)) => why,
        Err(CallerRefusal::Failed(e)) => panic!("expected a refusal, got a failure: {e:#}"),
        Err(CallerRefusal::Malformed(why)) => panic!("expected a refusal, got a malformed ask: {why}"),
        Ok(verified) => panic!("expected a refusal, got {}", verified.identity),
    }
}

/// A key from the stored list verifies and names its position; the
/// bearer slot works too; a wrong key and a missing key refuse.
#[sqlx::test]
async fn a_stored_key_admits_and_names_which_one(pool: PgPool) {
    schema(&pool).await;
    let spec = auth_spec("api_key_auth", &["keys"], json!({ "kind": "api_keys" }));
    let id = store(&pool, TENANT, spec, &[("keys", "first-key\nsecond-key\n")]).await;

    let ok = verify_caller(&verifiers(),&pool, &request(TENANT, &id, "api_key_auth", &[("x-api-key", "second-key")], b""), 0)
        .await
        .expect("the second key verifies");
    assert_eq!(ok.identity, json!({ "key": 1 }));
    assert_eq!(ok.credential_headers, ["X-Api-Key", "authorization"], "the run never writes the key down");
    let ok = verify_caller(&verifiers(),&pool, &request(TENANT, &id, "api_key_auth", &[("authorization", "Bearer first-key")], b""), 0)
        .await
        .expect("the bearer slot works");
    assert_eq!(ok.identity, json!({ "key": 0 }));

    let why = refused(verify_caller(&verifiers(),&pool, &request(TENANT, &id, "api_key_auth", &[("x-api-key", "nope")], b""), 0).await);
    assert!(why.contains("does not match"), "{why}");
    let why = refused(verify_caller(&verifiers(),&pool, &request(TENANT, &id, "api_key_auth", &[], b""), 0).await);
    assert!(why.contains("X-Api-Key"), "{why}");
}

/// An HMAC over the exact body with the connection's secret verifies
/// (an anonymous identity); a tampered body refuses.
#[sqlx::test]
async fn a_signed_body_admits_with_the_stored_secret(pool: PgPool) {
    schema(&pool).await;
    let verify = json!({
        "kind": "hmac",
        "signature": { "in": "header:X-Signature" },
        "timestamp": { "in": "header:X-Timestamp" },
        "concat": "{timestamp}.{body}"
    });
    let spec = auth_spec("hmac_auth", &["signingSecret"], verify.clone());
    let id = store(&pool, TENANT, spec, &[("signingSecret", "s3cret")]).await;
    let now = 1_700_000_000;
    let body = br#"{"text":"hi"}"#;
    let signature = sign(&verify, "s3cret", Some("1700000000"), body, &[], None);
    let headers = [("x-signature", signature.as_str()), ("x-timestamp", "1700000000")];
    let ok = verify_caller(&verifiers(),&pool, &request(TENANT, &id, "hmac_auth", &headers, body), now)
        .await
        .expect("a genuine signature verifies");
    assert_eq!(ok.identity, json!({}));
    let why = refused(verify_caller(&verifiers(),&pool, &request(TENANT, &id, "hmac_auth", &headers, b"{}"), now).await);
    assert!(why.contains("does not match"), "{why}");
}

/// The digest `verify` expects under `secret`, as a sender writes it.
fn sign(
    verify: &serde_json::Value,
    secret: &str,
    timestamp: Option<&str>,
    body: &[u8],
    headers: &[(String, String)],
    url: Option<&str>,
) -> String {
    let kind: weft_core::access::events::VerifyKind = serde_json::from_value(verify.clone()).expect("a valid scheme");
    weft_core::access::verify::hmac_digest(
        &kind,
        secret,
        timestamp,
        &weft_core::access::verify::PushParts { body, headers, url, method: "POST" },
    )
    .expect("signs")
}

/// Twilio's scheme gating a Route: the signature covers the full
/// address the caller was told to call, query included, so the gate
/// verifies only with the url the door hands it. The vector is Twilio's
/// own SDK test (twilio-python, tests/unit/test_request_validator.py).
#[sqlx::test]
async fn a_route_gated_by_a_url_signing_scheme_verifies_the_address_called(pool: PgPool) {
    schema(&pool).await;
    let spec = auth_spec(
        "twilio_auth",
        &["signingSecret"],
        json!({
            "kind": "hmac",
            "signature": { "in": "header:X-Twilio-Signature" },
            "concat": "{url}{sorted_form_params}",
            "algorithm": "sha1",
            "encoding": "base64"
        }),
    );
    let id = store(&pool, TENANT, spec, &[("signingSecret", "12345")]).await;
    let body = b"CallSid=CA1234567890ABCDE&Digits=1234&From=%2B14158675309&To=%2B18005551212&Caller=%2B14158675309";
    let headers = [("x-twilio-signature", "RSOYDt4T1cUTdK1PDd93/VVr8B8=")];
    let mut req = request(TENANT, &id, "twilio_auth", &headers, body);
    req.url = Some("https://mycompany.com/myapp.php?foo=1&bar=2".into());
    let ok = verify_caller(&verifiers(), &pool, &req, 0).await.expect("the address called verifies");
    assert_eq!(ok.identity, json!({}));

    req.url = Some("https://mycompany.com/myapp.php".into());
    let why = refused(verify_caller(&verifiers(), &pool, &req, 0).await);
    assert!(why.contains("does not match"), "{why}");
    req.url = None;
    let why = refused(verify_caller(&verifiers(), &pool, &req, 0).await);
    assert!(why.contains("public receiver address"), "{why}");
}

/// The Standard Webhooks scheme (Svix, Resend, Whop), as a node author
/// declares it in their own access node, gating a Route, against the
/// reference library's own vector; a signature header sent twice gives
/// the gate every value, so the right one verifies whichever copy
/// carries it.
#[sqlx::test]
async fn a_standard_webhooks_signature_admits_from_any_header_copy(pool: PgPool) {
    schema(&pool).await;
    let spec = auth_spec(
        "standard_webhook_auth",
        &["signingSecret"],
        json!({
            "kind": "hmac",
            "signature": { "in": "header:webhook-signature", "list": " ", "pair": ",", "key": "v1" },
            "timestamp": { "in": "header:webhook-timestamp" },
            "concat": "{header:webhook-id}.{timestamp}.{body}",
            "secret": { "stripPrefix": "whsec_", "decode": "base64" },
            "encoding": "base64"
        }),
    );
    spec.validate().expect("the recipe is valid");
    let id = store(&pool, TENANT, spec, &[("signingSecret", "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")]).await;
    let body = br#"{"test": 2432232314}"#;
    let ts = 1614265330;
    let base = [("webhook-id", "msg_p5jXN8AQM9LWM0D4loKWxJek"), ("webhook-timestamp", "1614265330")];
    let with = |signatures: &[&str]| {
        let mut h: Vec<(&str, &str)> = base.to_vec();
        h.extend(signatures.iter().map(|s| ("webhook-signature", *s)));
        request(TENANT, &id, "standard_webhook_auth", &h, body)
    };
    let ok = verify_caller(&verifiers(), &pool, &with(&["v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="]), ts)
        .await
        .expect("the reference vector verifies");
    assert_eq!(ok.identity, json!({}));
    assert_eq!(ok.credential_headers, ["webhook-signature"]);
    verify_caller(&verifiers(), &pool, &with(&["v1,bm90IGl0", "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="]), ts)
        .await
        .expect("the second copy of the header verifies");
    let why = refused(verify_caller(&verifiers(), &pool, &with(&["v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OA="]), ts).await);
    assert!(why.contains("does not match"), "{why}");
    let why = refused(verify_caller(&verifiers(), &pool, &with(&["v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="]), ts + 301).await);
    assert!(why.contains("replay"), "{why}");
}

/// The tenant wall, a wrong service, and a service with no `verify`
/// block all refuse (a failure of ours is the only non-refusal).
#[sqlx::test]
async fn the_wall_the_service_and_a_missing_scheme_all_refuse(pool: PgPool) {
    schema(&pool).await;
    let spec = auth_spec("api_key_auth", &["keys"], json!({ "kind": "api_keys" }));
    let id = store(&pool, TENANT, spec, &[("keys", "k")]).await;
    let headers = [("x-api-key", "k")];

    let why = refused(verify_caller(&verifiers(),&pool, &request("tenant-b", &id, "api_key_auth", &headers, b""), 0).await);
    assert!(why.contains("no such connection"), "{why}");

    match verify_caller(&verifiers(),&pool, &request(TENANT, &id, "other_service", &headers, b""), 0).await {
        Err(CallerRefusal::Failed(e)) => assert!(format!("{e:#}").contains("other_service"), "{e:#}"),
        other => panic!("a wrong service is a loud failure, got {other:?}"),
    }

    let plain: AccessSpec = serde_json::from_value(json!({
        "service": "plain",
        "acquisition": { "kind": "static", "fields": [{ "name": "token" }] }
    }))
    .unwrap();
    let plain_id = store(&pool, TENANT, plain, &[("token", "t")]).await;
    let why = refused(verify_caller(&verifiers(),&pool, &request(TENANT, &plain_id, "plain", &headers, b""), 0).await);
    assert!(why.contains("no `verify` block"), "{why}");

    match verify_caller(&verifiers(),&pool, &request(TENANT, "not-a-uuid", "api_key_auth", &headers, b""), 0).await {
        Err(CallerRefusal::Malformed(why)) => assert!(why.contains("not-a-uuid"), "{why}"),
        other => panic!("a malformed id is the asker's mistake, got {other:?}"),
    }
}

/// An identity-token gate, as a node author declares it in their own
/// access node: the connection stores the issuer, its key address and the
/// audience.
fn jwt_spec() -> AccessSpec {
    let spec = auth_spec(
        "jwt_auth",
        &["issuer", "jwksUrl", "audience"],
        json!({ "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }),
    );
    spec.validate().expect("the recipe is valid");
    spec
}

/// A JWT gate needs an audience: the issuer signs tokens for every app it
/// serves, and the audience is the only thing telling this gate's apart.
/// A connection cannot be saved without one.
#[sqlx::test]
async fn a_jwt_gate_connection_cannot_be_saved_without_an_audience(pool: PgPool) {
    schema(&pool).await;
    let unsaved = connect_direct(
        &pool,
        TENANT,
        ConnectDirect {
            paste: false,
            spec: jwt_spec(),
            door: Door::Own,
            registration: None,
            values: [("issuer", "https://auth.example"), ("jwksUrl", "https://auth.example/jwks.json")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            label: Some("gate".into()),
            permissions: vec![],
            project_id: None,
            instance: None,
        },
    )
    .await
    .expect_err("no audience, no connection");
    assert!(format!("{unsaved:#}").contains("audience"), "{unsaved:#}");
}

/// A JWT gate whose stored key address is plain http is refused before
/// any key is fetched: keys fetched over http could be swapped by anyone
/// on the path.
#[sqlx::test]
async fn a_jwt_gate_with_an_http_key_address_is_refused(pool: PgPool) {
    schema(&pool).await;
    let values = [("issuer", "https://auth.example"), ("jwksUrl", "http://auth.example/jwks.json"), ("audience", "my-app")];
    let id = store(&pool, TENANT, jwt_spec(), &values).await;
    let why = refused(
        verify_caller(&verifiers(), &pool, &request(TENANT, &id, "jwt_auth", &[("authorization", "Bearer a.b.c")], b""), 0).await,
    );
    assert!(why.contains("not an https:// address"), "{why}");
}
