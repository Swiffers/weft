//! Format self-tests: the pieces a template reads as, how values read
//! inside it, the templates it refuses as malformed, and the two
//! port/hole mismatches the node refuses at run time.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult};

use super::{holes, parse_template, render, FormatNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("holes_are_named_once_in_order", names),
        NodeTest::basic("rendering_writes_strings_as_is_and_the_rest_as_json", renders),
        NodeTest::basic("doubled_braces_are_text_and_a_broken_hole_refuses", braces),
        NodeTest::basic("the_compilers_placeholder_pattern_reads_the_holes_the_node_reads", compiler_agrees),
        NodeTest::fake("the_node_fills_from_its_wired_ports", node_fills),
        NodeTest::fake("a_hole_without_a_port_and_a_port_without_a_hole_both_refuse", node_refuses),
    ]
}

fn ports(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn names() -> WeftResult<()> {
    let pieces = parse_template("Hi {{user}}, {{ count }} left, {{user}} again")?;
    assert_eq!(holes(&pieces), vec!["user", "count"]);
    assert!(holes(&parse_template("no holes { here }")?).is_empty());
    Ok(())
}

/// The compiler checks the holes against the ports with the pattern the
/// metadata declares (`placeholdersAreInputs`), knowing nothing of
/// templates; this holds it to the node's own reading, doubled braces
/// included, so the build and the run never disagree on a template.
fn compiler_agrees() -> WeftResult<()> {
    use weft::NodeManifest;
    let spec = FormatNode.manifest().placeholders_are_inputs.as_ref().expect("Format declares its placeholders");
    let regex = spec.regex().map_err(weft::node_error)?;
    for template in [
        "Hi {{user}}, {{ count }} left, {{user}} again",
        "{{a}}{{b}}",
        "answer as {{{{json}}}} for {{who}}",
        "}}}} and {{{{ stay text, {{x}} does not",
        "no holes { here }",
    ] {
        let node = holes(&parse_template(template)?);
        let compiler = spec.names_in(&regex, template).map_err(weft::node_error)?;
        assert_eq!(compiler, node, "{template}");
    }
    Ok(())
}

fn renders() -> WeftResult<()> {
    let number = parse_template("{{n}}")?;
    for (value, expected) in [(json!(41), "41"), (json!(41.0), "41"), (json!(-0.0), "0"), (json!(1.25), "1.25")] {
        assert_eq!(render(&number, &ports(&[("n", value)])), expected);
    }
    let pieces = parse_template("Hi {{user}}: {{n}} items, {{items}}, flag {{ok}}, {{user}}!")?;
    let text = render(
        &pieces,
        &ports(&[
            ("user", json!("Ann")),
            ("n", json!(3)),
            ("items", json!(["a", "b"])),
            ("ok", json!(true)),
        ]),
    );
    assert_eq!(text, "Hi Ann: 3 items, [\"a\",\"b\"], flag true, Ann!", "a repeated hole fills every time");
    Ok(())
}

fn braces() -> WeftResult<()> {
    // Doubled braces are text and ask for no port, so a prompt can
    // name another system's placeholder.
    let pieces = parse_template("Answer with {{{{json}}}} only, {{user}}")?;
    assert_eq!(holes(&pieces), vec!["user"]);
    assert_eq!(
        render(&pieces, &ports(&[("user", json!("Ann"))])),
        "Answer with {{json}} only, Ann"
    );

    let unclosed = parse_template("Hi {{user").expect_err("an unclosed hole refuses").to_string();
    assert!(unclosed.contains("never closes"), "{unclosed}");
    // A stray opening would otherwise be reported under a name nobody
    // wrote (`note: {{name`).
    let malformed = parse_template("{{ note: {{name}}").expect_err("a stray `{{` refuses").to_string();
    assert!(malformed.contains("not a port name"), "{malformed}");
    let empty = parse_template("nothing {{}} here").expect_err("an empty hole refuses").to_string();
    assert!(empty.contains("not a port name"), "{empty}");
    // A name the node could never declare as a port is refused here,
    // not later with advice to write `Format(first name: String)`.
    let spaced = parse_template("{{ first name }}").expect_err("a spaced name refuses").to_string();
    assert!(spaced.contains("not a port name"), "{spaced}");
    // A stray closing is refused like a stray opening, so an author
    // who meant two closing braces as text learns to double them, and
    // `}}}}` can only ever mean that.
    let stray = parse_template("{\"a\": {\"b\": 1}}").expect_err("a stray `}}` refuses").to_string();
    assert!(stray.contains("never opened"), "{stray}");
    // Single braces are text; only the closing pair needs doubling.
    assert_eq!(render(&parse_template("{\"a\": {\"b\": 1}}}}")?, &ports(&[])), "{\"a\": {\"b\": 1}}");
    Ok(())
}

async fn node_fills(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &FormatNode,
            json!({ "template": "Dear {{name}}, {{count}} left", "name": "Ann", "count": 2 }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["text"], json!("Dear Ann, 2 left"));
    Ok(())
}

async fn node_refuses(rig: FakeRig) -> WeftResult<()> {
    let missing = rig
        .run(&FormatNode, json!({ "template": "Dear {{name}}" }))
        .await
        .failure()?;
    assert!(missing.contains("{{name}}") && missing.contains("declare"), "{missing}");
    let unread = rig
        .run(&FormatNode, json!({ "template": "Dear friend", "name": "Ann" }))
        .await
        .failure()?;
    assert!(unread.contains("`name`") && unread.contains("never reads"), "{unread}");
    // The node's own setting is no port a hole reads: a wired template
    // reading `{{template}}` is refused, never filled with null.
    let own = rig.run(&FormatNode, json!({ "template": "Dear {{template}}" })).await.failure()?;
    assert!(own.contains("{{template}}") && own.contains("declare"), "{own}");
    Ok(())
}
