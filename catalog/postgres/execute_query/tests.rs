//! PostgresExecuteQuery self-tests: the pure parameter/identifier
//! plumbing, the placeholder reading that turns the node's ports into
//! the query's parameters (and the compiler's pattern that must find
//! the same names), what an answered result emits on `rows` and `row`
//! (through the rig's emit check, with the result handed in), and
//! everything the node must refuse BEFORE a dial (fake; the fake rig
//! has no sockets). The query path itself needs a real database and is
//! exercised end to end there.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::{FakeRig, Node, NodeManifest, NodeMetadata, NodeTest, RunContext, WeftResult, WeftType};

use tokio_postgres::types::Type;

use super::super::postgres::{params_of, placeholders, plan, quote_ident, spell_parameters, Plan, Statement};
use super::{answer, emit, PostgresExecuteQueryNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("params_map_every_json_shape", params_map),
        NodeTest::basic("identifiers_quote_and_refuse_empty", idents),
        NodeTest::basic("placeholders_become_positions_in_first_use_order", placeholder_order),
        NodeTest::basic("placeholders_leave_quotes_comments_and_dollar_bodies_alone", placeholder_quoting),
        NodeTest::basic("placeholders_count_statements_and_refuse_positions", placeholder_statements),
        NodeTest::basic("plan_reads_the_query_as_a_query_or_a_script", plan_mismatches),
        NodeTest::basic("a_refusal_names_the_port_not_the_position", spelled_parameters),
        NodeTest::basic("placeholders_and_the_metadata_pattern_agree", pattern_agrees),
        NodeTest::basic("a_result_fills_rows_row_and_count", answers),
        NodeTest::fake("no_rows_sends_an_empty_list_and_closes_row", no_rows),
        NodeTest::fake("no_rows_sends_null_on_a_row_typed_to_admit_it", no_rows_nullable),
        NodeTest::fake("a_declared_record_that_fits_passes", declared_fits),
        NodeTest::fake("a_row_that_does_not_fit_fails_naming_the_field", declared_misfits),
        NodeTest::fake("a_non_numeric_port_refuses_before_dialing", bad_port),
        NodeTest::fake("an_unknown_tls_setting_refuses_before_dialing", bad_tls),
        NodeTest::fake("an_unread_port_refuses_before_dialing", unread_port),
        NodeTest::fake("a_script_with_a_placeholder_still_needs_the_port", script_with_placeholder),
        NodeTest::fake("a_placeholder_without_a_port_refuses_before_dialing", missing_port),
        NodeTest::fake("a_declared_port_that_stayed_silent_is_not_a_refusal", silent_optional_port),
        NodeTest::fake("a_refused_query_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_query_comes_out_on_error_when_it_is_wired", refused_wired),
        NodeTest::fake("a_bad_connection_setting_fails_the_run_even_with_error_wired", bad_setting_wired),
        NodeTest::fake("a_mistake_in_the_sql_fails_the_run_even_with_error_wired", sql_mistake_wired),
    ]
}

/// `photo?: String` on a card sent with no photo link: the author said the
/// value may be absent, so the query binds SQL NULL instead of refusing.
/// The refusal is the thing under test, so this asserts the node got
/// PAST the hole check; the dial itself has no socket in a fake rig.
async fn silent_optional_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.input_type("photo", weft::WeftType::parse("String").expect("String parses"));
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "INSERT INTO cards (photo) VALUES ($photo)",
            }),
        )
        .await
        .result
        .expect_err("a fake rig has no socket to dial");
    let err = err.to_string();
    assert!(!err.contains("`$photo`"), "the hole check refused a declared port: {err}");
    Ok(())
}

fn placeholder_order() -> WeftResult<()> {
    let p = placeholders("SELECT $b, $a, $b::int FROM t WHERE x = $a")?;
    assert_eq!(p.statements.len(), 1);
    assert_eq!(p.statements[0].sql, "SELECT $1, $2, $1::int FROM t WHERE x = $2");
    assert_eq!(p.statements[0].names, vec!["b", "a"]);
    // No placeholder at all is a plain query.
    let none = placeholders("SELECT 1")?;
    assert_eq!(none.statements, vec![Statement { sql: "SELECT 1".into(), names: vec![] }]);
    // Each statement numbers its own ports from $1, and a port two
    // statements read binds once per statement.
    let two = placeholders("UPDATE t SET n = $n WHERE id = $id; SELECT * FROM t WHERE id = $id")?;
    assert_eq!(two.statements.len(), 2);
    assert_eq!(two.statements[0].sql, "UPDATE t SET n = $1 WHERE id = $2");
    assert_eq!(two.statements[0].names, vec!["n", "id"]);
    assert_eq!(two.statements[1].sql, "SELECT * FROM t WHERE id = $1");
    assert_eq!(two.statements[1].names, vec!["id"]);
    assert_eq!(two.names(), vec!["n", "id"]);
    Ok(())
}

fn placeholder_quoting() -> WeftResult<()> {
    // A `$` inside a string, a quoted identifier, either comment, a
    // dollar-quoted body, or an identifier is SQL, never a parameter;
    // only the real one moves.
    let sql = "SELECT '$not', \"$col\", x$y, E'\\'$no', $$ $body; $$, $fn$ $inner; $fn$ \
               -- $line; \n /* $block; /* nested */ */ FROM t WHERE id = $real";
    let p = placeholders(sql)?;
    assert_eq!(p.statements.len(), 1, "the `;` inside quotes and comments separate nothing");
    let s = &p.statements[0];
    assert_eq!(s.names, vec!["real"], "{}", s.sql);
    assert!(s.sql.ends_with("id = $1"), "{}", s.sql);
    assert!(s.sql.contains("'$not'") && s.sql.contains("\"$col\"") && s.sql.contains("x$y"));
    assert!(s.sql.contains("$$ $body; $$") && s.sql.contains("$fn$ $inner; $fn$"));
    assert!(s.sql.contains("-- $line;") && s.sql.contains("/* $block; /* nested */ */"));
    // Unclosed quoting is a loud error, not a silent parameter.
    assert!(placeholders("SELECT $$ open").is_err());
    assert!(placeholders("SELECT 'open").is_err());
    Ok(())
}

fn placeholder_statements() -> WeftResult<()> {
    let script = placeholders("CREATE TABLE a (x int);\nCREATE TABLE b (y int);\n")?;
    assert_eq!(script.statements.len(), 2, "a trailing `;` counts no empty statement");
    assert_eq!(placeholders("DO $$ BEGIN NULL; END $$;")?.statements.len(), 1);
    let err = placeholders("SELECT * FROM t WHERE id = $1").expect_err("positions are gone").to_string();
    assert!(err.contains("$1") && err.contains("Declare the port"), "{err}");
    // A name that could never be a port is the same refusal.
    let err = placeholders("SELECT * FROM t WHERE id = $1st").expect_err("not a port name").to_string();
    assert!(err.contains("$1st"), "{err}");
    // In a SCRIPT that binds no port nothing is renumbered, so `$1` is
    // the author's own (a PREPARE) and stays.
    let script = placeholders("PREPARE find AS SELECT * FROM t WHERE id = $1; EXECUTE find(3)")?;
    assert_eq!(script.statements.len(), 2);
    assert!(
        script.statements[0].sql.contains("$1") && script.names().is_empty(),
        "{}",
        script.statements[0].sql
    );
    // A script that binds a port cannot also carry a `$1`.
    let err = placeholders("INSERT INTO t VALUES ($x); PREPARE f AS SELECT $1")
        .expect_err("mixed")
        .to_string();
    assert!(err.contains("mixes") && err.contains("$1"), "{err}");
    // Nothing to run is a refusal, not an empty answer.
    let err = placeholders("-- fill this in later").expect_err("an empty query is refused").to_string();
    assert!(err.contains("empty"), "{err}");
    Ok(())
}

fn plan_mismatches() -> WeftResult<()> {
    assert_eq!(
        plan("UPDATE t SET n = $n WHERE id = $id")?,
        Plan::Query {
            sql: "UPDATE t SET n = $1 WHERE id = $2".into(),
            names: vec!["n".into(), "id".into()],
        },
        "the ports bind in placeholder order, whatever order they arrived in"
    );
    assert_eq!(
        plan("CREATE TABLE a (x int); CREATE TABLE b (y int)")?,
        Plan::Script {
            head: vec!["CREATE TABLE a (x int)".into()],
            last: "CREATE TABLE b (y int)".into(),
        },
        "no port named anywhere: the leading statements go whole, the last one alone so \
         its rows come back typed"
    );
    assert_eq!(
        plan("CREATE TABLE a (x int); INSERT INTO a VALUES ($x)")?,
        Plan::Steps(vec![
            Statement { sql: "CREATE TABLE a (x int)".into(), names: vec![] },
            Statement { sql: "INSERT INTO a VALUES ($1)".into(), names: vec!["x".into()] },
        ]),
        "a port named in a script: statement by statement, each with its own ports"
    );
    Ok(())
}

/// Encoding a parameter against a column type is what proves the
/// binding: a wrong one refuses here exactly as Postgres would. A
/// count of parameters would pass whatever the bindings were.
fn binds(value: serde_json::Value, ty: &Type) -> bool {
    let mut buf = bytes::BytesMut::new();
    params_of(&[value])[0].to_sql_checked(ty, &mut buf).is_ok()
}

fn params_map() -> WeftResult<()> {
    assert!(binds(json!(true), &Type::BOOL), "a bool binds as bool");
    assert!(binds(json!("text"), &Type::TEXT), "a string binds as text");
    assert!(binds(json!({ "a": 1 }), &Type::JSONB), "an object binds as jsonb");

    // A string binds against ANY column, in text format, so a cast
    // placeholder (`$when::timestamptz`) and a uuid column both take
    // it as Postgres takes a quoted literal: the bytes sent are the
    // text itself.
    for ty in [Type::TIMESTAMPTZ, Type::UUID, Type::NUMERIC, Type::JSONB, Type::DATE] {
        assert!(binds(json!("2024-05-01T10:00:00Z"), &ty), "a string binds to a {ty} column");
    }
    let mut buf = bytes::BytesMut::new();
    let text = params_of(&[json!("2024-05-01T10:00:00Z")]).remove(0);
    text.to_sql_checked(&Type::TIMESTAMPTZ, &mut buf).expect("binds");
    assert_eq!(&buf[..], b"2024-05-01T10:00:00Z", "sent as text, parsed by the database");
    assert!(matches!(text.encode_format(&Type::TIMESTAMPTZ), tokio_postgres::types::Format::Text));
    assert!(matches!(params_of(&[json!(1)]).remove(0).encode_format(&Type::INT4), tokio_postgres::types::Format::Binary), "numbers stay binary");

    // A number binds to the column it meets, because that is what the
    // author wrote it for. `id serial primary key` is int4, so a rule
    // that bound whole numbers as int8 alone would refuse the single
    // commonest query there is (`WHERE id = $1`).
    for ty in [Type::INT2, Type::INT4, Type::INT8, Type::FLOAT4, Type::FLOAT8] {
        assert!(binds(json!(7), &ty), "a whole number binds to a {ty} column");
    }
    assert!(binds(json!(4.5), &Type::FLOAT8), "a fractional number binds as float8");

    // A value the column cannot hold is REFUSED, never wrapped: an id
    // that arrived truncated would match or overwrite a different row.
    assert!(!binds(json!(70_000), &Type::INT2), "a value past int2 refuses");
    assert!(!binds(json!(4.5), &Type::INT4), "a fraction is not a whole number");

    // An id past what a float can hold exactly keeps its value.
    let exact = 9_007_199_254_740_993_i64; // 2^53 + 1
    let mut buf = bytes::BytesMut::new();
    params_of(&[json!(exact)])[0]
        .to_sql_checked(&Type::INT8, &mut buf)
        .expect("binds as int8");
    assert_eq!(i64::from_be_bytes(buf[..].try_into().expect("8 bytes")), exact);
    Ok(())
}

fn idents() -> WeftResult<()> {
    assert_eq!(quote_ident("users")?, "\"users\"");
    assert_eq!(quote_ident("we\"ird")?, "\"we\"\"ird\"");
    assert!(quote_ident("  ").is_err(), "an empty identifier refuses");
    Ok(())
}

/// The connection fields every dial needs, so each refusal test
/// varies exactly the one field it is about.
fn connection(rig: &FakeRig) {
    rig.connection_value("postgres", "host", "db.example.com");
    rig.connection_value("postgres", "database", "app");
    rig.connection_value("postgres", "user", "postgres");
    rig.connection_value("postgres", "password", "secret");
}

async fn run_query(rig: &FakeRig) -> weft::RigOutcome {
    rig.run(
        &PostgresExecuteQueryNode,
        json!({
            "account": rig.access("postgres"),
            "query": "SELECT 1",
        }),
    )
    .await
}

async fn bad_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("postgres", "port", "not-a-number");
    let err = run_query(&rig).await.result.expect_err("a bad port must refuse").to_string();
    assert!(err.contains("port is not a number"), "{err}");
    Ok(())
}

async fn unread_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({ "account": rig.access("postgres"), "query": "SELECT 1", "who": "u1" }),
        )
        .await
        .result
        .expect_err("a wired port the SQL never reads must refuse")
        .to_string();
    assert!(err.contains("`who`") && err.contains("never reads"), "{err}");
    Ok(())
}

/// A script that names a port binds it like a single query does, so
/// the port has to arrive; with it missing the refusal names it, and
/// nothing is dialed either way (the fake rig has no sockets, so a
/// plan that passed the port check fails at the dial, which proves
/// the script was accepted).
async fn script_with_placeholder(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "CREATE TABLE a (x int); INSERT INTO a VALUES ($x)",
            }),
        )
        .await
        .result
        .expect_err("the named port must arrive")
        .to_string();
    assert!(err.contains("`$x`") && err.contains("declare"), "{err}");
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "CREATE TABLE a (x int); INSERT INTO a VALUES ($x)",
                "x": 1,
            }),
        )
        .await
        .result
        .expect_err("the fake rig has no database to dial")
        .to_string();
    assert!(!err.contains("script"), "the script itself was accepted: {err}");
    Ok(())
}

/// A `$name` the ports never carried is named, with the header to
/// write. The mirror of `unread_port`.
async fn missing_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "SELECT * FROM users WHERE id = $userId",
            }),
        )
        .await
        .result
        .expect_err("a placeholder with no port must refuse")
        .to_string();
    assert!(err.contains("`$userId`") && err.contains("declare"), "{err}");
    Ok(())
}

async fn bad_tls(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("postgres", "sslmode", "sometimes");
    let err = run_query(&rig).await.result.expect_err("a bad TLS setting must refuse").to_string();
    assert!(err.contains("TLS setting"), "{err}");
    Ok(())
}

/// A database refusal in the author's own words. Postgres counts
/// parameters; the author named them, and the number it prints appears
/// nowhere in their file. The one that costs the most time is a
/// parameter the server cannot place: it fails at run time, deep in
/// whatever loop was running, and the fix (a cast) is not guessable
/// from the text.
fn spelled_parameters() -> WeftResult<()> {
    let names: Vec<String> = ["id", "rude", "sure", "reason"].iter().map(|s| s.to_string()).collect();

    let cannot_place = spell_parameters(
        "db error: ERROR: could not determine data type of parameter $4",
        &names,
    );
    assert!(cannot_place.contains("$reason"), "{cannot_place}");
    assert!(!cannot_place.contains("$4"), "the number is replaced, not appended: {cannot_place}");
    assert!(cannot_place.contains("::text"), "the fix is named: {cannot_place}");

    // The other spelling, from a message about the VALUE rather than
    // the SQL. Same answer, so the author never meets a bare number.
    let serializing = spell_parameters("error serializing parameter 2", &names);
    assert_eq!(serializing, "error serializing parameter $rude");

    // A number that is not a parameter index stays exactly as written:
    // a dollar-quoted body, a position past the end, a plain count.
    let untouched = spell_parameters("syntax error near $$ and $9, 3 rows", &names);
    assert_eq!(untouched, "syntax error near $$ and $9, 3 rows");

    // Nothing named means nothing to say back, and the message has to
    // survive that unchanged rather than come out mangled.
    let no_ports = spell_parameters("ERROR: relation \"cards\" does not exist", &[]);
    assert_eq!(no_ports, "ERROR: relation \"cards\" does not exist");
    Ok(())
}

/// A database that hangs up on every connection, so the failure is the
/// connection's, an outcome of this run, which is what both `error`
/// tests read.
async fn refused_query(rig: &FakeRig) -> weft::RigOutcome {
    connection(rig);
    rig.connection_value("postgres", "host", "127.0.0.1");
    rig.connection_value("postgres", "port", &rig.hang_up_server().to_string());
    rig.connection_value("postgres", "sslmode", "disable");
    run_query(rig).await
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    let err = refused_query(&rig).await.failure()?;
    assert!(err.contains("postgres: connect"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let outcome = refused_query(&rig).await.ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("postgres: connect"), "{error}");
    assert!(!outcome.outputs.contains_key("rows") && !outcome.outputs.contains_key("count"));
    Ok(())
}

/// A mistake in the program's own SQL (a positional `$1`) is fixed by
/// editing the program, so it fails the run even with `error` wired.
async fn sql_mistake_wired(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.wire_output("error");
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({ "account": rig.access("postgres"), "query": "SELECT * FROM t WHERE id = $1" }),
        )
        .await
        .failure()?;
    assert!(err.starts_with("input error") && err.contains("`$1`"), "{err}");
    Ok(())
}

/// A connection set up wrong is the program's own shape, never an
/// outcome to route around: it fails the run even with `error` wired.
async fn bad_setting_wired(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("postgres", "sslmode", "sometimes");
    rig.wire_output("error");
    let err = run_query(&rig).await.failure()?;
    assert!(err.contains("TLS setting"), "{err}");
    Ok(())
}

/// The compiler reads a query's placeholders with the metadata's
/// `placeholdersAreInputs` pattern, the node with `placeholders()`;
/// they must find the same names, or a query the node runs fails the
/// build (or one it refuses passes it). Each query here is one the
/// node accepts, chosen for a shape where the two could part.
///
/// One shape is known to part, and it is not in this list: a block
/// comment NESTED in another (`/* a /* b */ $x */`). Comments nest to
/// any depth, which no regular pattern can follow, so the pattern ends
/// the comment at the first `*/` and `$x` reads as a placeholder the
/// node would never read.
fn pattern_agrees() -> WeftResult<()> {
    let spec = PostgresExecuteQueryNode
        .manifest()
        .placeholders_are_inputs
        .as_ref()
        .expect("the node declares placeholdersAreInputs");
    assert_eq!(spec.setting, "query");
    let regex = spec.regex().map_err(weft::WeftError::NodeFailed)?;
    let cases: &[(&str, &[&str])] = &[
        ("SELECT $b, $a, $b::int FROM t WHERE x = $a", &["b", "a"]),
        ("SELECT * FROM users WHERE id = $userId AND at > $when::timestamptz", &["userId", "when"]),
        // Strings: `''` escapes a quote, so the string goes on past it.
        ("SELECT 'it''s $no', $yes", &["yes"]),
        // `E'...'` escapes with a backslash too, upper or lower case.
        ("SELECT E'a\\'$no\\\\', e'$no2''$no3', $yes", &["yes"]),
        // A quote after a word ending in `e` is a plain string, where a
        // backslash escapes nothing.
        ("SELECT some'a\\', $e", &["e"]),
        // After a `$` too: `a$e` is one identifier and `$E` a
        // placeholder, so the quote after either opens a plain string.
        ("SELECT a$e'x\\', $E'\\', $w", &["E", "w"]),
        ("SELECT \"we$ird\", \"a\"\"$b\", $c", &["c"]),
        // A `$` inside an identifier is part of the name.
        ("SELECT x$y, col$1, a$b$c, $z FROM t", &["z"]),
        ("SELECT 1 -- $no\n, $a /* $no */ , $b", &["a", "b"]),
        ("DO $$ BEGIN PERFORM $no; END $$; SELECT $yes", &["yes"]),
        ("SELECT $$a$$||$tail", &["tail"]),
        ("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $body$ SELECT $no; $body$; SELECT f(), $x", &["x"]),
        ("SELECT $a$ $inside $a$, $b", &["b"]),
        // A `$tag$` body runs to its OWN closing tag: another tag inside
        // it, and what sits between that pair, is text.
        ("CREATE FUNCTION f() AS $fn$ BEGIN EXECUTE $q$ SELECT $zz $q$; END $fn$; SELECT $a", &["a"]),
        ("SELECT $f$ one $g$ $zz $f$, $a", &["a"]),
        // A positional `$1` names no input (a script's own PREPARE).
        ("PREPARE p AS SELECT $1; EXECUTE p(1)", &[]),
        // A name read twice, and read by two statements, is one input.
        ("UPDATE t SET n = $n WHERE id = $id; SELECT * FROM t WHERE id = $id", &["n", "id"]),
        // The query the placeholder-quoting test reads.
        (
            "SELECT '$not', \"$col\", x$y, E'\\'$no', $$ $body; $$, $fn$ $inner; $fn$ \
             -- $line; \n /* $block; /* nested */ */ FROM t WHERE id = $real",
            &["real"],
        ),
    ];
    for (query, expected) in cases {
        let node = placeholders(query)?.names();
        let compiler = spec.names_in(&regex, query).map_err(weft::WeftError::NodeFailed)?;
        assert_eq!(node, *expected, "the node's reading of {query:?}");
        assert_eq!(compiler, *expected, "the metadata pattern's reading of {query:?}");
    }
    Ok(())
}

fn answers() -> WeftResult<()> {
    let ada = json!({ "id": "1", "name": "ada" });
    let bo = json!({ "id": "2", "name": "bo" });

    let none = answer(Vec::new(), false).outputs;
    assert_eq!(none.get("rows"), Some(&json!([])), "no rows is an empty list, sent");
    assert_eq!(none.get("count"), Some(&json!(0.0)));
    assert!(!none.contains_key("row"), "no rows leaves `row` out, which closes it");
    assert_eq!(answer(Vec::new(), true).outputs.get("row"), Some(&Value::Null), "unless it takes null");

    let one = answer(vec![ada.clone()], false).outputs;
    assert_eq!(one.get("row"), Some(&ada));
    assert_eq!(one.get("rows"), Some(&json!([ada.clone()])));

    let several = answer(vec![ada.clone(), bo.clone()], true).outputs;
    assert_eq!(several.get("row"), Some(&ada), "the FIRST row, whatever `row` takes");
    assert_eq!(several.get("rows"), Some(&json!([ada, bo])), "and every row on `rows`");
    assert_eq!(several.get("count"), Some(&json!(2.0)));
    Ok(())
}

/// The node's own emission over a result handed in, under the node's
/// own manifest: what a query answering these rows sends, through the
/// rig's emit check, with no database behind it.
struct Answered(Vec<Value>);

impl NodeManifest for Answered {
    fn manifest(&self) -> &'static NodeMetadata {
        PostgresExecuteQueryNode.manifest()
    }
}

#[async_trait]
impl Node for Answered {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        emit(&ctx, self.0.clone()).await
    }
}

fn ty(text: &str) -> WeftType {
    WeftType::parse(text).unwrap_or_else(|| panic!("{text} parses"))
}

async fn no_rows(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&Answered(Vec::new()), json!({})).await.ok()?;
    assert_eq!(outcome.output("rows")?, &json!([]));
    assert!(!outcome.outputs.contains_key("row"), "`row` closes: {:?}", outcome.outputs);
    Ok(())
}

async fn no_rows_nullable(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("row", ty("JsonDict | Null"));
    let outcome = rig.run(&Answered(Vec::new()), json!({})).await.ok()?;
    assert_eq!(outcome.output("row")?, &Value::Null);
    assert_eq!(outcome.output("rows")?, &json!([]), "`rows` is filled from the same result");
    Ok(())
}

/// `-> (row: User)` and `-> (rows: List[User])` with an open `User`:
/// the rows fit, and the columns `User` does not name are dropped.
async fn declared_fits(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("row", ty("{ id: String, name: String | Null, .. }"));
    rig.output_type("rows", ty("List[{ id: String, name: String | Null, .. }]"));
    let rows = vec![
        json!({ "id": "1", "name": "ada", "joined": "2024-05-01" }),
        json!({ "id": "2", "name": null, "joined": "2024-06-01" }),
    ];
    let outcome = rig.run(&Answered(rows), json!({})).await.ok()?;
    assert_eq!(outcome.output("row")?, &json!({ "id": "1", "name": "ada" }));
    assert_eq!(
        outcome.output("rows")?,
        &json!([{ "id": "1", "name": "ada" }, { "id": "2", "name": null }])
    );
    Ok(())
}

async fn declared_misfits(rig: FakeRig) -> WeftResult<()> {
    rig.output_type("row", ty("{ id: String, email: String, .. }"));
    let err = rig
        .run(&Answered(vec![json!({ "id": "1", "name": "ada" })]), json!({}))
        .await
        .failure()?;
    assert!(err.contains("missing required field \"email\""), "{err}");
    Ok(())
}
