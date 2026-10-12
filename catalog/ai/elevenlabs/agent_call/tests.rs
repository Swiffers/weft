//! ElevenLabsAgentCall self-tests: the provider-routed dial (Twilio,
//! Exotel, SIP trunk), every per-call setting reaching the request,
//! the JSON client data merging under the typed inputs, and the
//! refusals.

use serde_json::{json, Value};

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use super::ElevenLabsAgentCallNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("dials_through_the_numbers_provider", dials),
        NodeTest::fake("every_per_call_setting_reaches_the_request", every_setting),
        NodeTest::fake("the_client_data_json_merges_under_the_typed_inputs", client_data_merges),
        NodeTest::fake("an_exotel_number_dials_through_exotel", exotel),
        NodeTest::fake("a_sip_trunk_call_answers_its_sip_call_id", sip_trunk),
        NodeTest::fake("a_phone_number_id_can_only_ever_name_a_number", number_id_encoded),
        NodeTest::fake("twilio_settings_on_another_number_are_refused_before_dialing", twilio_only),
        NodeTest::fake("a_number_from_an_unknown_provider_is_refused", unknown_provider),
        NodeTest::fake("a_soft_refusal_fails_loud", refused),
        NodeTest::live("one_real_outbound_call", "elevenlabs", live_call)
            .with_fixture(fixture_spec(
                "ELEVENLABS_PHONE_NUMBER_ID",
                "Phone number id",
                "An imported phone number's id on the connected account (Agents > Phone Numbers > the number's id). The call goes out FROM this number.",
            ))
            .with_fixture(fixture_spec(
                "ELEVENLABS_TO_NUMBER",
                "Number to call",
                "The E.164 number the test REALLY CALLS (your own phone): it rings once with a short test message; you can decline or hang up.",
            )),
    ]
}

/// One real outbound call: a throwaway agent is minted that allows a
/// per-call first message, the fixture number is REALLY DIALED with
/// that override (announce itself and hang up), the conversation is
/// read back through Get Conversation (no wait, so the test never
/// depends on the call being answered), and the agent is deleted
/// again. The conversation row the call leaves on the account IS the
/// proof and stays.
async fn live_call(rig: LiveRig) -> WeftResult<()> {
    let phone_number = rig.fixture("ELEVENLABS_PHONE_NUMBER_ID")?;
    let to_number = rig.fixture("ELEVENLABS_TO_NUMBER")?;
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;

    let created = rig
        .run(
            &crate::eleven_labs_create_agent::ElevenLabsCreateAgentNode,
            json!({
                "account": rig.access("elevenlabs"),
                "name": "weft-node-tests",
                "prompt": "You are a weft node test call for {{tester}}. Say exactly: this \
                           is an automated weft test call, goodbye. Then end the call.",
                "firstMessage": "This is a weft test call.",
                "voice": voice,
                "language": "en",
                "allowOverrides": ["agent.first_message"],
            }),
        )
        .await
        .ok()?;
    let agent = created.output("agentId")?.as_str().expect("agent id").to_string();

    weft::with_cleanup(
        || async {
            let outcome = rig
                .run(
                    &ElevenLabsAgentCallNode,
                    json!({
                        "account": rig.access("elevenlabs"),
                        "agent": agent.clone(),
                        "phoneNumber": phone_number,
                        "toNumber": to_number,
                        "dynamicVariables": { "tester": "weft" },
                        "firstMessage": "This is an automated weft test call. Goodbye!",
                        "ringingTimeoutSecs": 30,
                    }),
                )
                .await
                .ok()?;
            let conversation = outcome
                .output("conversationId")?
                .as_str()
                .expect("conversation id")
                .to_string();

            // Read the conversation WITHOUT waiting: the row exists the
            // moment the dial goes out, whatever the callee does.
            let read = rig
                .run(
                    &crate::eleven_labs_get_conversation::ElevenLabsGetConversationNode,
                    json!({
                        "account": rig.access("elevenlabs"),
                        "conversationId": conversation,
                        "waitUntilDone": false,
                        "includeAudio": false,
                    }),
                )
                .await
                .ok()?;
            let status = read.output("status")?.as_str().unwrap_or_default().to_string();
            assert!(
                ["initiated", "in-progress", "processing", "done", "failed"]
                    .contains(&status.as_str()),
                "the conversation reads back with a real status: {status}"
            );
            Ok(())
        },
        || async { crate::testing::delete_agent(&conn, &agent).await },
    )
    .await
}

/// The number `pn-1` from `provider`.
fn number(rig: &FakeRig, provider: &str) {
    rig.respond("GET", "/v1/convai/phone-numbers/pn-1", json!({ "phone_number_id": "pn-1", "provider": provider }));
}

/// The inputs of a call from `pn-1`, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "agent": "agent-1",
        "phoneNumber": "pn-1",
        "toNumber": "+15551234567",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

/// The JSON body of the call the node placed (the request after the
/// number's read).
fn call_body(rig: &FakeRig) -> Value {
    let requests = rig.requests();
    assert_eq!(requests.len(), 2, "one read of the number, one call");
    requests[1].body.clone().expect("json payload")
}

async fn dials(rig: FakeRig) -> WeftResult<()> {
    number(&rig, "twilio");
    rig.respond(
        "POST",
        "/v1/convai/twilio/outbound-call",
        json!({ "success": true, "message": "ok", "conversation_id": "conv-1", "callSid": "CA123" }),
    );
    let outcome = rig.run(&ElevenLabsAgentCallNode, inputs(&rig, json!({ "dynamicVariables": { "customer_name": "Ada" } }))).await.ok()?;
    assert_eq!(outcome.outputs["conversationId"], json!("conv-1"));
    assert_eq!(outcome.outputs["callSid"], json!("CA123"));
    assert!(!outcome.outputs.contains_key("sipCallId"), "a Twilio call has no SIP id");

    assert_eq!(
        call_body(&rig),
        json!({
            "agent_id": "agent-1",
            "agent_phone_number_id": "pn-1",
            "to_number": "+15551234567",
            "conversation_initiation_client_data": { "dynamic_variables": { "customer_name": "Ada" } },
        }),
        "nothing the program left unset is sent"
    );
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    number(&rig, "twilio");
    rig.respond(
        "POST",
        "/v1/convai/twilio/outbound-call",
        json!({ "success": true, "message": "ok", "conversation_id": "conv-1", "callSid": "CA123" }),
    );
    rig.run(
        &ElevenLabsAgentCallNode,
        inputs(
            &rig,
            json!({
                "dynamicVariables": { "customer_name": "Ada" },
                "firstMessage": "Hi Ada!",
                "prompt": "You confirm Ada's booking.",
                "language": "fr",
                "voice": "voice-2",
                "userId": "customer-42",
                "branchId": "branch-a",
                "environment": "staging",
                "ringingTimeoutSecs": 25,
                "recordCall": true,
                "answeringMachineDetection": "detect_message_end",
            }),
        ),
    )
    .await
    .ok()?;
    assert_eq!(
        call_body(&rig),
        json!({
            "agent_id": "agent-1",
            "agent_phone_number_id": "pn-1",
            "to_number": "+15551234567",
            "conversation_initiation_client_data": {
                "conversation_config_override": {
                    "agent": {
                        "first_message": "Hi Ada!",
                        "language": "fr",
                        "prompt": { "prompt": "You confirm Ada's booking." },
                    },
                    "tts": { "voice_id": "voice-2" },
                },
                "dynamic_variables": { "customer_name": "Ada" },
                "user_id": "customer-42",
                "branch_id": "branch-a",
                "environment": "staging",
            },
            "telephony_call_config": {
                "ringing_timeout_secs": 25,
                "twilio_call_recording_enabled": true,
                "twilio_machine_detection": { "mode": "detect_message_end" },
            },
        })
    );
    Ok(())
}

/// `clientData` reaches ElevenLabs whole, and the typed inputs go on
/// top: objects merge key by key, and the typed value wins where both
/// set the same key.
async fn client_data_merges(rig: FakeRig) -> WeftResult<()> {
    number(&rig, "twilio");
    rig.respond(
        "POST",
        "/v1/convai/twilio/outbound-call",
        json!({ "success": true, "message": "ok", "conversation_id": "conv-1", "callSid": "CA123" }),
    );
    rig.run(
        &ElevenLabsAgentCallNode,
        inputs(
            &rig,
            json!({
                "firstMessage": "Typed first message.",
                "dynamicVariables": { "slot": "19:00" },
                "clientData": {
                    "conversation_config_override": {
                        "agent": { "first_message": "Overwritten." },
                        "tts": { "stability": 0.3 },
                    },
                    "dynamic_variables": { "slot": "18:00", "table": "4" },
                    "starting_workflow_node_id": "node-7",
                    "user_id": "kept-user",
                },
            }),
        ),
    )
    .await
    .ok()?;
    assert_eq!(
        call_body(&rig)["conversation_initiation_client_data"],
        json!({
            "conversation_config_override": {
                "agent": { "first_message": "Typed first message." },
                "tts": { "stability": 0.3 },
            },
            "dynamic_variables": { "slot": "19:00", "table": "4" },
            "starting_workflow_node_id": "node-7",
            "user_id": "kept-user",
        })
    );
    Ok(())
}

async fn exotel(rig: FakeRig) -> WeftResult<()> {
    number(&rig, "exotel");
    rig.respond(
        "POST",
        "/v1/convai/exotel/outbound-call",
        json!({ "success": true, "message": "ok", "conversation_id": "conv-3", "callSid": "EX9" }),
    );
    let outcome = rig.run(&ElevenLabsAgentCallNode, inputs(&rig, json!({ "ringingTimeoutSecs": 40 }))).await.ok()?;
    assert_eq!(outcome.outputs["conversationId"], json!("conv-3"));
    assert_eq!(outcome.outputs["callSid"], json!("EX9"), "Exotel's CallSid");
    assert_eq!(rig.requests()[1].path, "/v1/convai/exotel/outbound-call");
    assert_eq!(call_body(&rig)["telephony_call_config"], json!({ "ringing_timeout_secs": 40 }));
    Ok(())
}

async fn sip_trunk(rig: FakeRig) -> WeftResult<()> {
    number(&rig, "sip_trunk");
    rig.respond(
        "POST",
        "/v1/convai/sip-trunk/outbound-call",
        json!({ "success": true, "message": "ok", "conversation_id": "conv-4", "sip_call_id": "sip-77" }),
    );
    let outcome = rig.run(&ElevenLabsAgentCallNode, inputs(&rig, json!({}))).await.ok()?;
    assert_eq!(outcome.outputs["conversationId"], json!("conv-4"));
    assert_eq!(outcome.outputs["sipCallId"], json!("sip-77"));
    assert!(!outcome.outputs.contains_key("callSid"), "a SIP call has no CallSid");
    assert_eq!(rig.requests()[1].path, "/v1/convai/sip-trunk/outbound-call");
    Ok(())
}

/// A typed number id that looks like a path stays one path segment, so
/// it cannot reach another ElevenLabs route.
async fn number_id_encoded(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", "/v1/convai/phone-numbers/..%2Fagents", json!({ "provider": "twilio" }));
    rig.respond(
        "POST",
        "/v1/convai/twilio/outbound-call",
        json!({ "success": true, "message": "ok", "conversation_id": "conv-5", "callSid": "CA5" }),
    );
    rig.run(&ElevenLabsAgentCallNode, inputs(&rig, json!({ "phoneNumber": "../agents" }))).await.ok()?;
    assert_eq!(rig.requests()[0].path, "/v1/convai/phone-numbers/..%2Fagents");
    Ok(())
}

/// Recording and answering machine detection are Twilio's: on another
/// number ElevenLabs ignores them, so the node refuses them once the
/// number's provider is known, before anything is dialed.
async fn twilio_only(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    rig.respond("GET", "/v1/convai/phone-numbers/pn-exotel", json!({ "provider": "exotel" }));
    rig.respond("GET", "/v1/convai/phone-numbers/pn-sip", json!({ "provider": "sip_trunk" }));
    for (extra, says) in [
        (json!({ "phoneNumber": "pn-exotel", "recordCall": true }), "an Exotel number, and `recordCall`"),
        (
            json!({ "phoneNumber": "pn-sip", "answeringMachineDetection": "enable" }),
            "a SIP trunk number, and `answeringMachineDetection`",
        ),
    ] {
        let err = rig.run(&ElevenLabsAgentCallNode, inputs(&rig, extra.clone())).await.failure()?;
        assert!(err.starts_with("input error"), "a mistake in the program fails the run even with `error` wired: {extra}: {err}");
        assert!(err.contains(says), "{extra}: {err}");
    }
    assert!(rig.requests().iter().all(|r| r.method == "GET"), "only the numbers were read; nothing was dialed");
    Ok(())
}

async fn unknown_provider(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    number(&rig, "carrier_pigeon");
    let err = rig.run(&ElevenLabsAgentCallNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.starts_with("input error"), "{err}");
    assert!(err.contains("provider 'carrier_pigeon'"), "{err}");
    assert_eq!(rig.requests().len(), 1, "nothing was dialed");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    number(&rig, "sip_trunk");
    rig.respond(
        "POST",
        "/v1/convai/sip-trunk/outbound-call",
        json!({ "success": false, "message": "agent has no voice configured", "conversation_id": null, "sip_call_id": null }),
    );
    let outcome = rig.run(&ElevenLabsAgentCallNode, inputs(&rig, json!({ "toNumber": "+15550000000" }))).await;
    let err = outcome.result.expect_err("a soft refusal must fail").to_string();
    assert!(err.contains("no voice configured"), "{err}");
    Ok(())
}
