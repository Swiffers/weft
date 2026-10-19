//! ComputerTools self-tests: the names each style declares, the
//! switches, and the shape every declaration takes.

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::styles::Style;
use super::{declarations, ComputerToolsNode, Offered};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("anthropic_declares_anthropics_names", anthropic_names),
        NodeTest::basic("openai_declares_openais_names", openai_names),
        NodeTest::basic("a_switch_off_drops_its_tools_but_never_the_screenshot", switched_off),
        NodeTest::basic("every_declaration_is_a_function_tool_with_an_object_schema", shapes),
        NodeTest::fake("the_node_declares_every_action_by_default", node_defaults),
        NodeTest::fake("an_unknown_style_fails_naming_the_styles", unknown_style),
    ]
}

fn names(tools: &[Value]) -> Vec<&str> {
    tools.iter().map(|t| t["name"].as_str().expect("a name")).collect()
}

fn anthropic_names() -> WeftResult<()> {
    let tools = declarations(Style::Anthropic, Offered::ALL);
    assert_eq!(
        names(&tools),
        vec![
            "screenshot", "zoom", "left_click", "right_click", "middle_click", "double_click", "triple_click",
            "left_click_drag", "mouse_move", "left_mouse_down", "left_mouse_up", "cursor_position", "scroll",
            "type", "key", "hold_key", "wait", "bash",
        ]
    );
    Ok(())
}

fn openai_names() -> WeftResult<()> {
    let tools = declarations(Style::OpenAi, Offered::ALL);
    assert_eq!(
        names(&tools),
        vec!["screenshot", "click", "double_click", "drag", "move", "scroll", "type", "keypress", "wait", "bash"]
    );
    Ok(())
}

fn switched_off() -> WeftResult<()> {
    let none = Offered {
        zoom: false,
        click: false,
        drag: false,
        r#move: false,
        mouse_buttons: false,
        cursor: false,
        scroll: false,
        typing: false,
        keys: false,
        wait: false,
        terminal: false,
    };
    assert_eq!(names(&declarations(Style::Anthropic, none)), vec!["screenshot"]);
    assert_eq!(names(&declarations(Style::OpenAi, none)), vec!["screenshot"]);
    let no_shell = Offered { terminal: false, keys: false, ..Offered::ALL };
    let anthropic = declarations(Style::Anthropic, no_shell);
    assert!(!names(&anthropic).iter().any(|n| ["bash", "key", "hold_key"].contains(n)), "{:?}", names(&anthropic));
    assert!(names(&anthropic).contains(&"type"), "typing stays on its own switch");
    Ok(())
}

fn shapes() -> WeftResult<()> {
    for style in [Style::Anthropic, Style::OpenAi] {
        for tool in declarations(style, Offered::ALL) {
            assert!(tool["description"].as_str().is_some_and(|d| !d.is_empty()), "{tool}");
            let parameters = &tool["parameters"];
            assert_eq!(parameters["type"], json!("object"), "{tool}");
            let properties = parameters["properties"].as_object().expect("properties");
            for required in parameters["required"].as_array().expect("required") {
                assert!(properties.contains_key(required.as_str().expect("a name")), "{tool} requires a field it does not declare");
            }
        }
    }
    Ok(())
}

async fn node_defaults(rig: FakeRig) -> WeftResult<()> {
    // Nothing set: the metadata's defaults (Anthropic's style, every
    // action on) are what the node declares.
    let outcome = rig.run(&ComputerToolsNode, json!({})).await.ok()?;
    let tools = outcome.outputs["tools"].as_array().expect("a list").clone();
    assert_eq!(names(&tools), names(&declarations(Style::Anthropic, Offered::ALL)));
    // A switch turned off is read from its own input.
    let outcome = rig.run(&ComputerToolsNode, json!({ "style": "openai", "terminal": false })).await.ok()?;
    let tools = outcome.outputs["tools"].as_array().expect("a list").clone();
    assert_eq!(names(&tools), vec!["screenshot", "click", "double_click", "drag", "move", "scroll", "type", "keypress", "wait"]);
    Ok(())
}

async fn unknown_style(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&ComputerToolsNode, json!({ "style": "gemini" })).await;
    let error = outcome.result.expect_err("no such style").to_string();
    assert!(error.contains("anthropic and openai"), "{error}");
    Ok(())
}
