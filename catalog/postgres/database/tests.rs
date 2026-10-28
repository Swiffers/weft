//! PostgresDatabase self-tests: the declared infrastructure, and the
//! two properties the whole design rests on. The running database
//! itself is exercised end to end.

use serde_json::json;

use weft::{EndpointMethod, FakeRig, NodeTest, WeftResult};

use super::{memory_mib, PostgresDatabaseNode, Tuning, CREDENTIAL_PATH, CREDENTIAL_STORED_PATH, LEAST_MEMORY_MIB, MOST_AT_ONCE};

/// The service this node declares it publishes, spelled here as the
/// test's own expectation rather than read from the node, so a change
/// to the declaration shows up as a failing test.
const SERVICE: &str = "postgres";

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("declares_a_database_that_reads_its_password_from_a_file", declares),
        NodeTest::fake("describes_itself_identically_every_time", stable),
        NodeTest::fake("its_machine_is_what_its_settings_say", sized),
        NodeTest::fake("keeps_the_credential_endpoint_to_the_project", internal),
        NodeTest::fake("waits_for_postgres_to_answer_for_itself", ready),
        NodeTest::basic("memory_reads_in_every_unit_the_input_takes", memory_units),
        NodeTest::basic("postgres_is_tuned_to_the_memory_it_has", tuned),
        NodeTest::basic("a_machine_too_small_for_one_worker_is_refused", too_small),
        NodeTest::basic("the_least_memory_is_where_one_worker_fits_and_the_input_says_so", least_memory),
        NodeTest::basic("a_full_database_is_told_by_the_runs_it_holds_back", full_waits),
        NodeTest::fake("postgres_runs_with_its_tuning_and_its_share_of_memory", runs_tuned),
        NodeTest::fake("the_first_run_reads_the_password_and_retires_it", first_run),
        NodeTest::fake("a_later_run_never_asks_the_database_again", later_run),
        NodeTest::fake("a_replaced_disk_makes_the_node_ask_again", replaced_disk),
        NodeTest::fake("an_answer_that_is_neither_yes_nor_no_fails", garbled),
        NodeTest::fake("a_refusal_from_the_credential_server_surfaces", refused),
    ]
}

fn config() -> serde_json::Value {
    json!({ "database": "app", "storage": "10Gi", "version": "17", "reachable": false, "cpu": "0.25", "memory": "1Gi" })
}

/// The same database, with the author saying something outside the
/// program may connect to it.
fn reachable_config() -> serde_json::Value {
    json!({ "database": "app", "storage": "10Gi", "version": "17", "reachable": true })
}

/// The two addresses the node's own infrastructure answers on. The
/// SQL one is only ever split into host and port, never dialled here.
fn endpoints(rig: &FakeRig) {
    rig.declare_endpoint("sql", "http://wi-db-sql:5432");
    rig.declare_endpoint("credential", "http://wi-db-credential:8099");
}

/// The password the node ends up publishing, whichever way it got it.
fn published_password(rig: &FakeRig) -> Option<String> {
    rig.published_values(SERVICE)?.get("password").cloned()
}

fn asked_for_the_password(rig: &FakeRig) -> bool {
    rig.endpoint_calls().iter().any(|c| {
        c.endpoint == "credential" && c.method == EndpointMethod::Get && c.path == CREDENTIAL_PATH
    })
}

/// What the node sent back to be retired, in order.
fn retired(rig: &FakeRig) -> Vec<String> {
    rig.endpoint_calls()
        .iter()
        .filter(|c| c.endpoint == "credential" && c.path == CREDENTIAL_STORED_PATH)
        .filter_map(|c| c.body.as_ref()?.get("password")?.as_str().map(str::to_string))
        .collect()
}

async fn declares(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(spec.units.len(), 1);
    let unit = &spec.units[0];
    assert_eq!(unit.init_containers.len(), 1, "the password is minted before Postgres boots");
    assert_eq!(unit.containers.len(), 2, "Postgres plus the server that hands the password over");
    let postgres = &unit.containers[0];
    assert!(
        postgres.env.iter().any(|e| matches!(e,
            weft::infra::EnvEntry { name, .. } if name == "POSTGRES_PASSWORD_FILE")),
        "Postgres reads the password from the shared volume"
    );
    assert!(
        !postgres.env.iter().any(|e| matches!(e,
            weft::infra::EnvEntry { name, .. } if name == "POSTGRES_PASSWORD")),
        "and never from a value carried in the spec"
    );
    assert_eq!(spec.volumes.len(), 2, "one disk for data and password, one scratch dir for the socket");
    assert!(
        matches!(spec.volumes[1].kind, weft::infra::VolumeKind::Scratch { .. }),
        "the socket never lands on the disk"
    );
    let socket_dir = "/var/run/postgresql";
    for container in &unit.containers {
        assert!(
            container.mounts.iter().any(|m| m.path == socket_dir && m.volume == "socket"),
            "{} shares the socket: it is how a password nobody holds gets replaced",
            container.name
        );
    }
    let credential = &unit.containers[1];
    for name in ["WEFT_SOCKET_DIR", "WEFT_ADMIN_USER"] {
        assert!(
            credential.env.iter().any(|e| matches!(e,
                weft::infra::EnvEntry { name: n, .. } if n == name)),
            "the credential server is told {name} by the node"
        );
    }
    Ok(())
}

/// The machine follows the node's settings, and an emptied machine
/// type leaves the pick to the cloud.
async fn sized(rig: FakeRig) -> WeftResult<()> {
    let mut settings = config();
    settings["cpu"] = json!("2");
    settings["memory"] = json!("8Gi");
    settings["machineType"] = json!("");
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, settings).await.ok()?;
    let machine = &outcome.infra_spec()?.units[0].machine;
    assert_eq!((machine.cpu.as_deref(), machine.memory.as_deref(), machine.kind.as_deref()), (Some("2"), Some("8Gi"), None));
    Ok(())
}

/// The rule the whole design rests on: describing the database twice
/// produces the same thing, so starting it again changes nothing. A
/// generated password in the spec would break this and take the
/// database's logins with it.
async fn stable(rig: FakeRig) -> WeftResult<()> {
    let first = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let second = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    assert_eq!(
        serde_json::to_value(first.infra_spec()?).expect("a spec serializes"),
        serde_json::to_value(second.infra_spec()?).expect("a spec serializes"),
        "two descriptions of the same database must be identical"
    );
    Ok(())
}

/// The rule every infra node lives by, on the node that has one of
/// each: an endpoint that HANDS OUT a credential is never open beyond the
/// project's own workers, whatever anybody asks for.
///
/// `sql` may be opened, because reaching Postgres there still costs a
/// password, and that is what lets a frontend on this machine use the
/// program's database instead of standing up a second one.
/// `credential` is the little server that MINTS that password: a door
/// onto it would hand the database to anything that could reach the
/// port, so it stays inside for ever and no input can open it.
async fn internal(rig: FakeRig) -> WeftResult<()> {
    // Nothing is reachable unless the author said so, and `reachable`
    // defaults off: a database on a machine somebody else can type on
    // is not something you get for not thinking about it.
    let closed = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    for endpoint in &closed.infra_spec()?.endpoints {
        assert!(
            matches!(endpoint.expose, weft::infra::Expose::Project),
            "'{}' is reachable on a database nobody asked to reach",
            endpoint.name
        );
    }

    // With it, exactly one endpoint opens. `credential` HANDS OUT the
    // password, so reaching it would give the database away to
    // anything that could reach the port; no input can open it.
    let open = rig.run_provision_infra(&PostgresDatabaseNode, reachable_config()).await.ok()?;
    for endpoint in &open.infra_spec()?.endpoints {
        let want_reachable = endpoint.name == "sql";
        assert_eq!(
            matches!(endpoint.expose, weft::infra::Expose::SameNetwork),
            want_reachable,
            "'{}' has the wrong exposure: only 'sql' may ever be reachable",
            endpoint.name
        );
    }
    Ok(())
}

/// Postgres accepts TCP while it is still starting up and refuses
/// every connection, so the port being bound is not readiness. The
/// probe has to ask the database the question it answers itself.
async fn ready(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let spec = outcome.infra_spec()?;
    let postgres = &spec.units[0].containers[0];
    let probe = postgres.readiness.as_ref().expect("Postgres declares a readiness probe");
    let weft::infra::ProbeKind::Exec { command } = &probe.kind else {
        panic!("readiness must run the database's own check, not a port test: {:?}", probe.kind);
    };
    assert!(command.iter().any(|a| a == "pg_isready"), "{command:?}");
    // Over TCP, not the unix socket: the entrypoint runs a temporary
    // socket-only server while it initialises, so a socket check
    // reports ready while the port is still closed.
    assert!(command.iter().any(|a| a == "-h"), "the check must go over TCP: {command:?}");
    // A database that always exists: without `-d` the check signs in to
    // one named after the user, and the server logs that it is missing
    // on every check.
    assert!(command.windows(2).any(|w| w == ["-d", "postgres"]), "{command:?}");
    Ok(())
}

fn memory_units() -> WeftResult<()> {
    assert_eq!(memory_mib("1Gi")?, 1024);
    assert_eq!(memory_mib("1.5Gi")?, 1536);
    assert_eq!(memory_mib("512Mi")?, 512);
    // A gigabyte is 10^9 bytes, a little under 954 MiB, rounded down so
    // nothing is planned for memory the machine does not have.
    assert_eq!(memory_mib("1G")?, 953);
    assert_eq!(memory_mib("1500M")?, 1430);
    assert!(memory_mib("1GB").is_err());
    assert!(memory_mib("Gi").is_err());
    Ok(())
}

/// The default machine, worked through by hand from the formula: 1024
/// MiB less 320 for the rest of the machine leaves 704; a quarter of
/// that is the shared cache, a sixteenth each cleaner's, and the 396
/// left over holds 24 connections at 16 MiB each.
fn tuned() -> WeftResult<()> {
    assert_eq!(
        Tuning::for_machine("1Gi")?,
        Tuning {
            limit_mib: 704,
            max_connections: 24,
            shared_buffers_mib: 176,
            work_mem_kib: 4352,
            maintenance_work_mem_mib: 44,
            effective_cache_size_mib: 528,
        }
    );
    // More memory, more connections, each still given about Postgres's
    // own 4MB to sort in.
    let two = Tuning::for_machine("2Gi")?;
    assert_eq!(two.max_connections, 60);
    assert!((4096..4400).contains(&two.work_mem_kib), "{two:?}");
    // Past the most connections Postgres takes, the memory goes to each
    // connection's sorting instead.
    let big = Tuning::for_machine("64Gi")?;
    assert_eq!(big.max_connections, 200);
    assert_eq!(big.maintenance_work_mem_mib, 1024, "a vacuum uses at most 1GB");
    assert!(big.work_mem_kib > 100 * 1024, "{big:?}");
    // Whatever the machine, everything the settings allow at once fits
    // in Postgres's share.
    for memory in ["1Gi", "1.5Gi", "2Gi", "4Gi", "8Gi", "16Gi", "64Gi"] {
        let t = Tuning::for_machine(memory)?;
        let at_once_kib = t.shared_buffers_mib * 1024
            + 3 * t.maintenance_work_mem_mib * 1024
            + t.max_connections * (8 * 1024 + 2 * t.work_mem_kib);
        assert!(at_once_kib <= t.limit_mib * 1024, "{memory}: {t:?}");
    }
    Ok(())
}

fn too_small() -> WeftResult<()> {
    let refused = Tuning::for_machine("512Mi").expect_err("512Mi holds fewer connections than one worker opens");
    let floor = format!("raise `memory` to {LEAST_MEMORY_MIB}Mi or more");
    assert!(matches!(&refused, weft::WeftError::Input(m) if m.contains(&floor)), "{refused:?}");
    Ok(())
}

/// The notice about a full database counts runs, each once however
/// many times it is refused, says how many wait now, and comes at most
/// once a minute.
fn full_waits() -> WeftResult<()> {
    let t0 = std::time::Instant::now();
    let mut waits = super::super::postgres::FullWaits::default();
    waits.begin();
    waits.begin();
    let first = waits.notice(t0).expect("the first refusal is told");
    assert!(first.contains("2 run(s) wait for one now, 2 began waiting"), "{first}");
    assert_eq!(waits.notice(t0 + std::time::Duration::from_secs(30)), None, "once a minute at most");
    waits.waiting -= 1;
    let later = waits.notice(t0 + std::time::Duration::from_secs(61)).expect("a minute later");
    assert!(later.contains("1 run(s) wait for one now, 0 began waiting"), "{later}");
    Ok(())
}

/// The floor the refusal names is the real one (one MiB less is
/// refused, it is not), and the `memory` input's description states
/// the same number and connection count.
fn least_memory() -> WeftResult<()> {
    assert_eq!(Tuning::for_machine(&format!("{LEAST_MEMORY_MIB}Mi"))?.max_connections, MOST_AT_ONCE as u64);
    assert!(Tuning::for_machine(&format!("{}Mi", LEAST_MEMORY_MIB - 1)).is_err());
    let metadata: serde_json::Value = serde_json::from_str(include_str!("metadata.json")).expect("metadata.json reads");
    let memory = metadata["inputs"]
        .as_array()
        .and_then(|inputs| inputs.iter().find(|i| i["name"] == "memory"))
        .and_then(|i| i["description"].as_str())
        .expect("a memory input with a description");
    let says = format!("at least {LEAST_MEMORY_MIB}Mi, the least that holds the {MOST_AT_ONCE} connections");
    assert!(memory.contains(&says), "the description says '{says}': {memory}");
    Ok(())
}

async fn runs_tuned(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&PostgresDatabaseNode, config()).await.ok()?;
    let spec = outcome.infra_spec()?;
    let postgres = &spec.units[0].containers[0];
    assert_eq!(postgres.args, Tuning::for_machine("1Gi")?.args());
    assert!(postgres.args.windows(2).any(|w| w == ["-c", "max_connections=24"]), "{:?}", postgres.args);
    assert_eq!(postgres.limits.memory.as_deref(), Some("704Mi"));
    Ok(())
}

/// The first run: nothing published yet, so the password comes from
/// the database, gets published, and is retired only afterwards.
async fn first_run(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.answer_endpoint("credential", EndpointMethod::Get, CREDENTIAL_PATH, json!({ "password": "minted" }));
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": true }));

    rig.run(&PostgresDatabaseNode, config()).await.ok()?;

    assert!(asked_for_the_password(&rig), "the first run has nowhere else to get it");
    assert_eq!(published_password(&rig).as_deref(), Some("minted"));
    let values = rig.published_values(SERVICE).expect("a connection was published");
    assert_eq!(values.get("host").map(String::as_str), Some("wi-db-sql"));
    assert_eq!(values.get("port").map(String::as_str), Some("5432"));
    assert_eq!(values.get("user").map(String::as_str), Some("weft"));
    assert_eq!(retired(&rig), vec!["minted".to_string()], "retired once, after publishing");

    // Retiring AFTER publishing is the whole safety property: were it
    // the other way round, a failure in between would leave a
    // database nothing can sign in to.
    let calls = rig.endpoint_calls();
    let read = calls.iter().position(|c| c.path == CREDENTIAL_PATH).expect("read");
    let retire = calls.iter().position(|c| c.path == CREDENTIAL_STORED_PATH).expect("retire");
    assert!(read < retire, "the password is read before it is retired");
    Ok(())
}

/// A later run holds the connection it published, so it never asks
/// the database for the password again. That is what lets the
/// database stop handing it out at all.
async fn later_run(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.published_connection(SERVICE, &[
        ("host", "wi-db-sql"),
        ("port", "5432"),
        ("database", "app"),
        ("user", "weft"),
        ("password", "minted"),
        ("sslmode", "disable"),
    ]);
    // One retire, and no answer at all for reading the password: were
    // the node to ask, the run would fail rather than quietly get
    // away with it.
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": true }));

    rig.run(&PostgresDatabaseNode, config()).await.ok()?;

    assert!(!asked_for_the_password(&rig), "the connection already holds it");
    assert_eq!(published_password(&rig).as_deref(), Some("minted"));
    Ok(())
}

/// A replaced disk means a fresh password, and the connection the
/// node holds is no longer the database's. It has to notice, rather
/// than go on handing out one nothing accepts.
async fn replaced_disk(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.published_connection(SERVICE, &[
        ("host", "wi-db-sql"),
        ("port", "5432"),
        ("database", "app"),
        ("user", "weft"),
        ("password", "from-the-old-disk"),
        ("sslmode", "disable"),
    ]);
    // The new disk disowns the old password, and offers its own. The
    // second retire, of the password it really made, is accepted.
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": false }));
    rig.answer_endpoint("credential", EndpointMethod::Get, CREDENTIAL_PATH, json!({ "password": "from-the-new-disk" }));
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "stored": true }));

    rig.run(&PostgresDatabaseNode, config()).await.ok()?;

    assert!(asked_for_the_password(&rig), "a password the database disowns is worthless");
    assert_eq!(published_password(&rig).as_deref(), Some("from-the-new-disk"));
    assert_eq!(
        retired(&rig),
        vec!["from-the-old-disk".to_string(), "from-the-new-disk".to_string()],
        "the stale one is offered first, which is how the node finds out"
    );
    Ok(())
}

/// An answer that is neither a yes nor a no is a broken peer, not a
/// no. Read as a no it would send the node looking for a fresh
/// password and, on a database that has already retired its own, all
/// the way to telling the user their data is unreachable.
async fn garbled(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.published_connection(SERVICE, &[("password", "minted")]);
    rig.answer_endpoint("credential", EndpointMethod::Post, CREDENTIAL_STORED_PATH, json!({ "error": "busy" }));

    let failure = rig.run(&PostgresDatabaseNode, config()).await.failure()?;
    assert!(failure.contains("neither a yes nor a no"), "{failure}");
    assert!(!asked_for_the_password(&rig), "a broken answer is not a reason to start over");
    Ok(())
}

/// The credential server refuses while it has no password on the disk
/// yet. That is a failure, not an answer: acting on it would mean
/// publishing a connection to a database whose password nobody knows.
async fn refused(rig: FakeRig) -> WeftResult<()> {
    endpoints(&rig);
    rig.refuse_endpoint(
        "credential",
        EndpointMethod::Get,
        CREDENTIAL_PATH,
        503,
        r#"{"error": "no password on the shared volume yet"}"#,
    );

    let failure = rig.run(&PostgresDatabaseNode, config()).await.failure()?;
    assert!(failure.contains("no password on the shared volume yet"), "{failure}");
    assert!(rig.published_values(SERVICE).is_none(), "nothing was published");
    Ok(())
}
