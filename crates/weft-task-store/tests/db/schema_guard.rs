//! Layer-3 tests for the schema guard against a REAL Postgres: the
//! stamp/verify/refuse behavior lives IN the SQL it runs, so a faked pool
//! would not catch it. Each test gets a fresh isolated database via
//! `#[sqlx::test]` (reads `$DATABASE_URL`, creates a random DB, drops it
//! after).

use sqlx::{AssertSqlSafe, PgPool};

use weft_task_store::schema_guard::apply_groups_with as apply;
use weft_task_store::{sql, Migration, Part, SchemaGroup};

/// Most of these tests are about the canonical DDL, so they run with no
/// migrations at all.
static NONE: &[Migration] = &[];

static GUARDED: SchemaGroup = SchemaGroup {
    name: "guard_probe",
    tables: &["guard_probe"],
    ddl: &[
        "CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_guard_probe_note ON guard_probe(note)",
    ],
    seed: &[],
    data_steps: &[],
};

static OTHER: SchemaGroup = SchemaGroup {
    name: "guard_other",
    tables: &["guard_other"],
    ddl: &["CREATE TABLE IF NOT EXISTS guard_other (id INT PRIMARY KEY)"],
    seed: &[],
    data_steps: &[],
};

#[sqlx::test]
async fn fresh_apply_stamps_and_reruns_are_idempotent(pool: PgPool) {
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("fresh apply");
    let (fp,): (String,) = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
    )
    .fetch_one(&pool)
    .await
    .expect("stamp row exists");
    assert_eq!(fp.len(), 64, "hex sha256 stamp");

    // Re-running with the same groups succeeds and leaves the stamp.
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("re-apply");
    let (fp2,): (String,) = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fp, fp2);

    // A matching stamp with a table gone builds the group again, so a
    // manually dropped table is repaired on the next boot.
    sqlx::raw_sql("DROP TABLE guard_probe").execute(&pool).await.unwrap();
    apply(&pool, &[&GUARDED], NONE).await.expect("repair apply");
    sqlx::query("SELECT 1 FROM guard_probe")
        .execute(&pool)
        .await
        .expect("table recreated");
}

#[sqlx::test]
async fn mismatched_fingerprint_fails_naming_group_and_drops(pool: PgPool) {
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("fresh apply");
    // Simulate a database created by a different build of the DDL: the
    // stamp differs AND the live shape genuinely disagrees with the
    // canonical build. (A stale stamp over a MATCHING shape is a
    // non-event: the boot proves the shape and restamps silently.)
    sqlx::query(
        "UPDATE weft_schema_stamp SET fingerprint = 'stale' WHERE group_name = 'guard_probe'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ALTER TABLE guard_probe ADD COLUMN sneaky TEXT")
        .execute(&pool)
        .await
        .unwrap();

    let err = apply(&pool, &[&GUARDED, &OTHER], NONE)
        .await
        .expect_err("stale stamp must fail the apply");
    let msg = format!("{err:#}");
    assert!(msg.contains("guard_probe"), "names the mismatched group: {msg}");
    assert!(
        msg.contains("DROP TABLE IF EXISTS guard_probe"),
        "prints the ready-to-run DROP: {msg}"
    );
    assert!(
        msg.contains("DELETE FROM weft_schema_stamp WHERE group_name = 'guard_probe';"),
        "prints the stamp reset: {msg}"
    );
    assert!(!msg.contains("guard_other"), "clean groups are not named: {msg}");

    // The failed run must not have restamped the mismatched group.
    let (fp,): (String,) = sqlx::query_as(
        "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fp, "stale");

    // Running the printed reset then re-applying recovers cleanly.
    sqlx::raw_sql(
        "DROP TABLE IF EXISTS guard_probe CASCADE;\
         DELETE FROM weft_schema_stamp WHERE group_name = 'guard_probe';",
    )
    .execute(&pool)
    .await
    .unwrap();
    apply(&pool, &[&GUARDED, &OTHER], NONE).await.expect("recovered apply");
}

/// A drift the shape check finds fails the boot and rolls back the
/// contract transaction it runs in: the contract parts it ran are not
/// recorded, and the drifted group keeps its old stamp. The expand work
/// before it stands, since the documents carried between the two may
/// have needed what it made: a group new to the database is built from
/// its canonical DDL and stamped.
#[sqlx::test]
async fn a_mismatch_rolls_back_the_contract_work_and_keeps_the_expand(pool: PgPool) {
    static DROP_NOTE_INDEX: &[Migration] = &[Migration {
        group: "guard_probe",
        id: "20260823T1300_drop_note_index.contract",
        part: Part::Contract,
        draft: false,
        sql: "DROP INDEX idx_guard_probe_note",
    }];
    apply(&pool, &[&GUARDED], NONE).await.expect("fresh apply");
    // A moved stamp AND a genuinely different live shape (see the
    // naming test above for why both are needed).
    sqlx::query(
        "UPDATE weft_schema_stamp SET fingerprint = 'stale' WHERE group_name = 'guard_probe'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ALTER TABLE guard_probe ADD COLUMN sneaky TEXT")
        .execute(&pool)
        .await
        .unwrap();

    apply(&pool, &[&GUARDED, &OTHER], DROP_NOTE_INDEX)
        .await
        .expect_err("the mismatch fails the boot");

    let stamp = |group: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT fingerprint FROM weft_schema_stamp WHERE group_name = $1")
                .bind(group)
                .fetch_optional(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(stamp("guard_probe").await.as_deref(), Some("stale"), "the drifted group is not restamped");
    assert!(stamp("guard_other").await.is_some(), "the new group was built in the expand work");
    sqlx::query("SELECT 1 FROM guard_other").execute(&pool).await.expect("its table stands");
    let ran: Option<String> = sqlx::query_scalar("SELECT id FROM weft_migration WHERE id = $1")
        .bind(DROP_NOTE_INDEX[0].id)
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert_eq!(ran, None, "the contract part rolled back with the failure");
    let index: Option<String> = sqlx::query_scalar("SELECT indexname::text FROM pg_indexes WHERE indexname = 'idx_guard_probe_note'")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(index.is_some(), "what the contract part dropped is back");
}

#[sqlx::test]
async fn concurrent_applies_serialize_on_the_advisory_lock(pool: PgPool) {
    // The exact scenario the lock exists for: two replicas booting the
    // same fresh database at once. IF NOT EXISTS alone is not
    // concurrency-safe in Postgres (both pass the existence check, one
    // dies on a duplicate catalog key); under the lock both must
    // succeed.
    let groups: [&SchemaGroup; 2] = [&GUARDED, &OTHER];
    let (a, b) = tokio::join!(apply(&pool, &groups, NONE), apply(&pool, &groups, NONE));
    a.expect("first concurrent apply");
    b.expect("second concurrent apply");
    sqlx::query("SELECT 1 FROM guard_probe").execute(&pool).await.expect("table exists");
}

// The same group after a change: a column added to the canonical DDL, and
// the migration that adds it to a database that already exists. The ALTER
// carries no IF NOT EXISTS on purpose, so a run that should have been
// skipped fails loudly instead of passing quietly.
static ADD_EXTRA: &[Migration] = &[Migration {
    group: "guard_probe",
    id: "20260823T1200_add_extra",
    part: Part::Expand,
    draft: false,
    sql: "ALTER TABLE guard_probe ADD COLUMN extra TEXT",
}];

static GUARDED_V2: SchemaGroup = SchemaGroup {
    name: "guard_probe",
    tables: &["guard_probe"],
    ddl: &[
        "CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT, extra TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_guard_probe_note ON guard_probe(note)",
    ],
    seed: &[],
    data_steps: &[],
};

async fn has_extra_column(pool: &PgPool) -> bool {
    sqlx::query_as::<_, (i32,)>(
        "SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'guard_probe' AND column_name = 'extra'",
    )
    .fetch_optional(pool)
    .await
    .unwrap()
    .is_some()
}

#[sqlx::test]
async fn a_database_that_already_exists_is_carried_forward_by_the_migration(pool: PgPool) {
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    assert!(!has_extra_column(&pool).await);
    sqlx::query("INSERT INTO guard_probe (id, note) VALUES (1, 'keep me')")
        .execute(&pool)
        .await
        .expect("a row somebody cares about");

    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("upgrade");
    assert!(has_extra_column(&pool).await, "the migration ran");

    // The whole point: upgrading carries the data across rather than
    // rebuilding the table around it.
    let (note,): (String,) = sqlx::query_as("SELECT note FROM guard_probe WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("the row is still there");
    assert_eq!(note, "keep me");

    let (id,): (String,) =
        sqlx::query_as("SELECT id FROM weft_migration WHERE group_name = 'guard_probe'")
            .fetch_one(&pool)
            .await
            .expect("the migration was recorded");
    assert_eq!(id, "20260823T1200_add_extra");

    // Booting again runs nothing a second time.
    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("re-apply");
}

#[sqlx::test]
async fn a_new_database_is_built_whole_and_inherits_the_history(pool: PgPool) {
    // Nothing to carry forward, so the canonical DDL builds the table with
    // the column already in it and the migration is recorded without running.
    // Running it here would fail on a duplicate column.
    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("fresh apply");
    assert!(has_extra_column(&pool).await);
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM weft_migration WHERE group_name = 'guard_probe'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1, "recorded as history it was born with");
}

#[sqlx::test]
async fn changing_the_ddl_with_no_migration_refuses(pool: PgPool) {
    static DRIFTED: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        ddl: &["CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT, extra TEXT)"],
        seed: &[],
        data_steps: &[],
    };
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    let err = apply(&pool, &[&DRIFTED], NONE).await.expect_err("must refuse");
    let msg = err.to_string();
    assert!(msg.contains("does not hold the shape"), "{msg}");
    assert!(msg.contains("guard_probe"), "{msg}");
    // The refusal names the exact object that differs, read from the
    // catalogs, not just the group.
    assert!(msg.contains("extra"), "{msg}");
    assert!(!has_extra_column(&pool).await, "the drifted DDL did not run");
}

/// A function or trigger is written with `CREATE OR REPLACE` and `DROP
/// ... IF EXISTS`, so running the new text would itself make the database
/// hold the new shape: the boot checks the database before running it, and
/// a body changed with no migration refuses like a column does.
#[sqlx::test]
async fn replacing_a_function_with_no_migration_refuses(pool: PgPool) {
    static V1: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        ddl: &[
            "CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT)",
            "CREATE OR REPLACE FUNCTION guard_probe_answer() RETURNS INT AS $$ SELECT 1 $$ LANGUAGE sql",
        ],
        seed: &[],
        data_steps: &[],
    };
    static V2: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        ddl: &[
            "CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT)",
            "CREATE OR REPLACE FUNCTION guard_probe_answer() RETURNS INT AS $$ SELECT 2 $$ LANGUAGE sql",
        ],
        seed: &[],
        data_steps: &[],
    };
    apply(&pool, &[&V1], NONE).await.expect("the old shape");
    let err = apply(&pool, &[&V2], NONE).await.expect_err("a replaced body must refuse");
    assert!(err.to_string().contains("does not hold the shape"), "{err}");
    let (answer,): (i32,) = sqlx::query_as("SELECT guard_probe_answer()").fetch_one(&pool).await.unwrap();
    assert_eq!(answer, 1, "the new body did not run");
    apply(&pool, &[&V1], NONE).await.expect("the unchanged code still boots");
}

/// A fingerprint that moved with NO shape change (a comment edit, a
/// seed statement moved out of the DDL) restamps silently: the shape
/// check proves the live schema already matches the canonical build,
/// so demanding a migration for a non-change would brick every
/// existing database over text.
#[sqlx::test]
async fn a_fingerprint_move_with_no_shape_change_restamps_silently(pool: PgPool) {
    static COMMENTED: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        // Same shape as GUARDED, different text.
        ddl: &[
            "-- a comment that moves the fingerprint\n\
             CREATE TABLE IF NOT EXISTS guard_probe (id INT PRIMARY KEY, note TEXT)",
            "CREATE INDEX IF NOT EXISTS idx_guard_probe_note ON guard_probe(note)",
        ],
        seed: &[],
        data_steps: &[],
    };
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    let stamp = |pool: PgPool| async move {
        let (fp,): (String,) = sqlx::query_as(
            "SELECT fingerprint FROM weft_schema_stamp WHERE group_name = 'guard_probe'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        fp
    };
    let before = stamp(pool.clone()).await;
    apply(&pool, &[&COMMENTED], NONE).await.expect("a text-only change boots clean");
    // And the stamp really moved, so later boots are a plain
    // stamp-match rather than a shape re-verification.
    assert_ne!(stamp(pool.clone()).await, before, "the shape check restamped");
    apply(&pool, &[&COMMENTED], NONE).await.expect("restamped");
}

/// A pending migration must never launder an unrelated DDL edit: the
/// migration runs, but the extra canonical column it does NOT create
/// still fails the boot loudly.
#[sqlx::test]
async fn a_pending_migration_does_not_launder_an_uncovered_ddl_edit(pool: PgPool) {
    static V2_PLUS_STOWAWAY: SchemaGroup = SchemaGroup {
        name: "guard_probe",
        tables: &["guard_probe"],
        // ADD_EXTRA's migration covers `extra`; `stowaway` has no
        // migration and IF NOT EXISTS will never add it.
        ddl: &["CREATE TABLE IF NOT EXISTS guard_probe \
                (id INT PRIMARY KEY, note TEXT, extra TEXT, stowaway TEXT)"],
        seed: &[],
        data_steps: &[],
    };
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    let err = apply(&pool, &[&V2_PLUS_STOWAWAY], ADD_EXTRA)
        .await
        .expect_err("the stowaway column must fail the boot");
    let msg = err.to_string();
    assert!(msg.contains("does not hold the shape"), "{msg}");
    assert!(msg.contains("stowaway"), "{msg}");
}

#[sqlx::test]
async fn editing_a_migration_that_already_ran_refuses(pool: PgPool) {
    static REWRITTEN: &[Migration] = &[Migration {
        group: "guard_probe",
        id: "20260823T1200_add_extra",
        part: Part::Expand,
        draft: false,
        sql: "ALTER TABLE guard_probe ADD COLUMN something_else TEXT",
    }];
    apply(&pool, &[&GUARDED], NONE).await.expect("the old shape");
    apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("upgrade");

    let err = apply(&pool, &[&GUARDED_V2], REWRITTEN).await.expect_err("must refuse");
    assert!(err.to_string().contains("edited after running"), "{err}");
}

// Two branches that each added a migration, merged in either order. Both
// files sit in the directory afterwards, and a database that has run one of
// them has to pick up the other, whichever way round.
static BRANCH_A: Migration = Migration {
    group: "guard_probe",
    id: "20260901T1000_from_branch_a",
    part: Part::Expand,
    draft: false,
    sql: "ALTER TABLE guard_probe ADD COLUMN from_a TEXT",
};
static BRANCH_B: Migration = Migration {
    group: "guard_probe",
    id: "20260902T1000_from_branch_b",
    part: Part::Expand,
    draft: false,
    sql: "ALTER TABLE guard_probe ADD COLUMN from_b TEXT",
};
static B_ONLY: &[Migration] = &[Migration { ..BRANCH_B }];
static BOTH: &[Migration] = &[Migration { ..BRANCH_A }, Migration { ..BRANCH_B }];

async fn columns(pool: &PgPool) -> Vec<String> {
    sqlx::query_as::<_, (String,)>(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'guard_probe' ORDER BY column_name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|(c,)| c)
    .collect()
}

#[sqlx::test]
async fn a_database_on_one_branch_picks_up_the_other_branchs_migration(pool: PgPool) {
    // This database followed branch B, so it has run the LATER id and never
    // saw the earlier one. Merging brings both files in, and the earlier one
    // still has to run: what a database owes is what it has not recorded, not
    // whatever sorts after the newest id it holds.
    apply(&pool, &[&GUARDED], NONE).await.expect("the shape both branched from");
    apply(&pool, &[&GUARDED], B_ONLY).await.expect("branch B");
    assert!(columns(&pool).await.contains(&"from_b".to_string()));
    assert!(!columns(&pool).await.contains(&"from_a".to_string()));

    apply(&pool, &[&GUARDED], BOTH).await.expect("after the merge");
    let after = columns(&pool).await;
    assert!(after.contains(&"from_a".to_string()), "branch A's migration ran late: {after:?}");
    assert!(after.contains(&"from_b".to_string()));

    // And it is recorded, so a third boot runs nothing.
    apply(&pool, &[&GUARDED], BOTH).await.expect("idempotent");
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM weft_migration WHERE group_name = 'guard_probe'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
}

// The generator has to write runnable SQL for every kind of change, including
// the destructive ones, rather than leaving a note asking somebody to write it.
#[sqlx::test]
async fn the_plan_writes_sql_for_a_drop_a_type_change_and_a_new_column(pool: PgPool) {
    use weft_task_store::schema_guard::{plan_migration, read_schema};

    sqlx::raw_sql(
        "CREATE TABLE probe (id INT PRIMARY KEY, doomed TEXT, count TEXT NOT NULL DEFAULT '0')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let before = read_schema(&pool).await.unwrap();

    sqlx::raw_sql(
        "DROP TABLE probe; \
         CREATE TABLE probe (id INT PRIMARY KEY, count BIGINT, added TEXT)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let after = read_schema(&pool).await.unwrap();

    let sql: String =
        plan_migration(&before, &after, &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect::<Vec<_>>().join("\n");
    assert!(sql.contains("ALTER TABLE probe DROP COLUMN doomed;"), "{sql}");
    assert!(sql.contains("ALTER TABLE probe ADD COLUMN added text;"), "{sql}");
    assert!(
        sql.contains("ALTER TABLE probe ALTER COLUMN count TYPE bigint USING count::bigint;"),
        "{sql}"
    );
    assert!(sql.contains("ALTER TABLE probe ALTER COLUMN count DROP DEFAULT;"), "{sql}");
    assert!(sql.contains("ALTER TABLE probe ALTER COLUMN count DROP NOT NULL;"), "{sql}");
    assert!(!sql.contains("yourself"), "nothing is left for a human to write: {sql}");
}

// Postgres names a constraint for every NOT NULL column. When the column
// goes, the plan must not also drop that constraint by name: the DROP
// COLUMN already took it, and the file would fail on a constraint that no
// longer exists. Proven by running the plan against the old table, which
// is exactly what a release does.
#[sqlx::test]
async fn dropping_a_not_null_column_does_not_also_drop_its_constraint(pool: PgPool) {
    use weft_task_store::schema_guard::{plan_migration, read_schema};

    let old = "CREATE TABLE probe (owner TEXT NOT NULL, big_key TEXT NOT NULL, payload TEXT NOT NULL, \
               PRIMARY KEY (owner, big_key))";
    sqlx::raw_sql(old).execute(&pool).await.unwrap();
    let before = read_schema(&pool).await.unwrap();

    sqlx::raw_sql(
        "DROP TABLE probe; \
         CREATE TABLE probe (owner TEXT NOT NULL, key_hash TEXT NOT NULL, payload TEXT NOT NULL, \
         PRIMARY KEY (owner, key_hash))",
    )
    .execute(&pool)
    .await
    .unwrap();
    let after = read_schema(&pool).await.unwrap();
    let stmts: Vec<String> =
        plan_migration(&before, &after, &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
    let sql = stmts.join("\n");
    assert!(sql.contains("DROP COLUMN big_key;"), "{sql}");
    assert!(!sql.contains("big_key_not_null"), "the column takes its NOT NULL constraint with it: {sql}");

    // The proof: the plan runs on the OLD table and lands on the new shape.
    sqlx::raw_sql(sql!("DROP TABLE probe; {old}")).execute(&pool).await.unwrap();
    for stmt in &stmts {
        sqlx::raw_sql(AssertSqlSafe(stmt.as_str())).execute(&pool).await.unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    let landed = read_schema(&pool).await.unwrap();
    assert_eq!(
        landed.iter().map(ToString::to_string).collect::<Vec<_>>(),
        after.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
}

// A function whose arguments changed is replaced in the expand part, old
// signature dropped and new one made: a call with the old arguments
// reaches the new one when its new argument has a default, where the old
// one kept beside it would make that call ambiguous.
#[sqlx::test]
async fn a_function_gaining_a_defaulted_argument_still_answers_its_old_callers(pool: PgPool) {
    use weft_task_store::schema_guard::{plan_migration, read_schema};

    let old = "CREATE FUNCTION probe_add(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$";
    sqlx::raw_sql(old).execute(&pool).await.unwrap();
    let before = read_schema(&pool).await.unwrap();
    sqlx::raw_sql(
        "DROP FUNCTION probe_add(integer); \
         CREATE FUNCTION probe_add(a integer, b integer DEFAULT 1) RETURNS integer LANGUAGE sql AS $$ SELECT a + b $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    let after = read_schema(&pool).await.unwrap();
    let plan = plan_migration(&before, &after, &[], &[]).unwrap();
    assert!(plan.iter().all(|p| p.part == Part::Expand), "nothing waits for the contract part");
    let sql = plan.iter().map(|p| p.stmt.as_str()).collect::<Vec<_>>().join("\n");
    assert!(sql.contains("DROP FUNCTION probe_add(integer);"), "{sql}");

    // Back to the old one, and the expand part run on it as a boot runs it:
    // the old arity is answered by the new function.
    sqlx::raw_sql(sql!("DROP FUNCTION probe_add(integer, integer); {old}")).execute(&pool).await.unwrap();
    sqlx::raw_sql(AssertSqlSafe(sql)).execute(&pool).await.unwrap();
    let answered: i32 = sqlx::query_scalar("SELECT probe_add(1)").fetch_one(&pool).await.expect("an older caller's call");
    assert_eq!(answered, 2);
    assert!(weft_task_store::schema_guard::diff_things(&after, "the code", &read_schema(&pool).await.unwrap(), "the database").is_none());
}

// A column added NOT NULL with no default arrives with no second NOT NULL
// constraint of its own (the ADD COLUMN made it), and one with a backfill
// is added holding it on the rows already there, the default then
// dropped, so a write that leaves it out fails.
#[sqlx::test]
async fn a_not_null_column_brings_its_own_constraint_and_its_backfill(pool: PgPool) {
    use weft_task_store::schema_guard::{plan_migration, read_schema, Backfill};

    let old = "CREATE TABLE probe (id INT PRIMARY KEY, body JSONB)";
    sqlx::raw_sql(old).execute(&pool).await.unwrap();
    let before = read_schema(&pool).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE probe; \
         CREATE TABLE probe (id INT PRIMARY KEY, body JSONB, body_version INTEGER NOT NULL, tags TEXT[] NOT NULL DEFAULT '{}'); \
         CREATE TABLE probe_new (k INT NOT NULL, name TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let after = read_schema(&pool).await.unwrap();
    let backfill = Backfill { table: "probe".into(), column: "body_version".into(), value: "1".into() };
    let plan = plan_migration(&before, &after, &[], &[backfill]).unwrap();
    let all = plan.iter().map(|p| p.stmt.as_str()).collect::<Vec<_>>().join("\n");
    assert!(!all.contains("_not_null"), "no NOT NULL constraint of its own: {all}");
    let contract: Vec<&str> = plan.iter().filter(|p| p.part == Part::Contract).map(|p| p.stmt.as_str()).collect();
    assert_eq!(contract, ["ALTER TABLE probe ALTER COLUMN body_version DROP DEFAULT;"]);

    sqlx::raw_sql(sql!("DROP TABLE probe; DROP TABLE probe_new; {old}; INSERT INTO probe (id) VALUES (1)"))
        .execute(&pool)
        .await
        .unwrap();
    for part in [Part::Expand, Part::Contract] {
        for stmt in plan.iter().filter(|p| p.part == part) {
            sqlx::raw_sql(AssertSqlSafe(stmt.stmt.as_str())).execute(&pool).await.unwrap_or_else(|e| panic!("{}: {e}", stmt.stmt));
        }
    }
    let version: i32 = sqlx::query_scalar("SELECT body_version FROM probe WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(version, 1, "the row already there holds the backfill");
    assert!(weft_task_store::schema_guard::diff_things(&after, "the code", &read_schema(&pool).await.unwrap(), "the database").is_none());
}

// Array and enum columns are the shapes information_schema renders as
// the words ARRAY / USER-DEFINED; the planner must emit their REAL type
// names, proven by executing what it wrote.
#[sqlx::test]
async fn the_plan_renders_array_and_enum_types_as_runnable_sql(pool: PgPool) {
    use weft_task_store::schema_guard::{plan_migration, read_schema};

    sqlx::raw_sql(
        "CREATE TYPE mood AS ENUM ('calm', 'stormy'); \
         CREATE TABLE probe (id INT PRIMARY KEY)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let before = read_schema(&pool).await.unwrap();

    sqlx::raw_sql(
        "ALTER TABLE probe ADD COLUMN tags TEXT[] NOT NULL DEFAULT '{}'::text[]; \
         ALTER TABLE probe ADD COLUMN m mood",
    )
    .execute(&pool)
    .await
    .unwrap();
    let after = read_schema(&pool).await.unwrap();

    let stmts: Vec<String> =
        plan_migration(&before, &after, &[], &[]).unwrap().into_iter().map(|p| p.stmt).collect();
    let sql = stmts.join("\n");
    assert!(sql.contains("text[]"), "array type by its real name: {sql}");
    assert!(sql.contains("mood"), "enum type by its real name: {sql}");
    assert!(!sql.contains("ARRAY") && !sql.contains("USER-DEFINED"), "{sql}");

    // The proof: roll the columns back and let the PLAN re-add them.
    sqlx::raw_sql("ALTER TABLE probe DROP COLUMN tags; ALTER TABLE probe DROP COLUMN m")
        .execute(&pool)
        .await
        .unwrap();
    for stmt in &stmts {
        sqlx::raw_sql(AssertSqlSafe(stmt.as_str())).execute(&pool).await.unwrap_or_else(|e| {
            panic!("planned SQL must run: {e}\n{stmt}");
        });
    }
    if let Some(diff) =
        weft_task_store::schema_guard::diff_things(&after, "the plan", &read_schema(&pool).await.unwrap(), "the database")
    {
        panic!("running the plan must land on the target shape:\n{diff}");
    }
}

/// The release path: `settle_release` is what `./setup.sh --migration
/// <name> --release` runs on the database you work against, in one
/// transaction, so what it records is only ever what the database
/// holds.
mod release {
    use super::*;
    use weft_task_store::schema_guard::{read_schema, settle_release, History, Settling};

    /// The draft a developer's database ran while the shape was being
    /// decided: the same change `ADD_EXTRA` releases.
    static DRAFT_EXTRA: &[Migration] = &[Migration {
        group: "guard_probe",
        id: "draft_20260823T1100_guard_probe_extra",
        part: Part::Expand,
        draft: true,
        sql: "ALTER TABLE guard_probe ADD COLUMN extra TEXT",
    }];

    async fn recorded_ids(pool: &PgPool) -> Vec<String> {
        sqlx::query_as::<_, (String,)>(
            "SELECT id FROM weft_migration WHERE group_name = 'guard_probe' AND id <> 'origin' ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(id,)| id)
        .collect()
    }

    fn release(run: bool) -> Vec<Settling> {
        vec![Settling {
            group: "guard_probe".into(),
            id: ADD_EXTRA[0].id.into(),
            part: Part::Expand,
            sql: ADD_EXTRA[0].sql.into(),
            run,
        }]
    }

    /// A database that ran the draft has the release RECORDED: the
    /// draft row goes, the released id is in, nothing runs, and the
    /// next boot with the released file has nothing to do.
    #[sqlx::test]
    async fn a_database_that_ran_the_drafts_records_the_release(pool: PgPool) {
        apply(&pool, &[&GUARDED], NONE).await.expect("origin");
        apply(&pool, &[&GUARDED_V2], DRAFT_EXTRA).await.expect("the draft ran");
        assert_eq!(recorded_ids(&pool).await, vec![DRAFT_EXTRA[0].id.to_string()]);
        let expected = read_schema(&pool).await.unwrap();

        settle_release(&History::embedded(), &pool, &[&GUARDED_V2], &[], &expected, &release(false)).await.expect("recorded");
        assert_eq!(recorded_ids(&pool).await, vec![ADD_EXTRA[0].id.to_string()]);
        apply(&pool, &[&GUARDED_V2], ADD_EXTRA).await.expect("nothing left to run");
    }

    /// A database that never saw the draft has the release RUN on it,
    /// and recorded, in the same transaction.
    #[sqlx::test]
    async fn a_database_that_never_saw_the_drafts_runs_the_release(pool: PgPool) {
        apply(&pool, &[&GUARDED], NONE).await.expect("origin");
        // The shape the release describes, read off the database once
        // it holds it, then put back to where the last release left it.
        sqlx::raw_sql(ADD_EXTRA[0].sql).execute(&pool).await.unwrap();
        let expected = read_schema(&pool).await.unwrap();
        sqlx::raw_sql("ALTER TABLE guard_probe DROP COLUMN extra").execute(&pool).await.unwrap();

        settle_release(&History::embedded(), &pool, &[&GUARDED_V2], &[], &expected, &release(true)).await.expect("ran");
        assert!(has_extra_column(&pool).await, "the release ran");
        assert_eq!(recorded_ids(&pool).await, vec![ADD_EXTRA[0].id.to_string()]);
    }

    /// A release whose SQL fails, or whose result is not the shape the
    /// code declares, records nothing: the draft rows are still there
    /// and the released id is not.
    #[sqlx::test]
    async fn a_release_that_fails_or_misses_the_shape_records_nothing(pool: PgPool) {
        apply(&pool, &[&GUARDED], NONE).await.expect("origin");
        apply(&pool, &[&GUARDED_V2], DRAFT_EXTRA).await.expect("the draft ran");
        let holds = read_schema(&pool).await.unwrap();

        // Running the SQL again on a database that already ran the
        // draft fails on the column that is there.
        let err = format!(
            "{:#}",
            settle_release(&History::embedded(), &pool, &[&GUARDED_V2], &[], &holds, &release(true))
                .await
                .expect_err("the column exists")
        );
        assert!(err.contains(ADD_EXTRA[0].id), "names the migration: {err}");
        assert_eq!(recorded_ids(&pool).await, vec![DRAFT_EXTRA[0].id.to_string()], "rolled back");

        // Recording against a shape the database does not hold is
        // refused naming the group, and the row is not written.
        let mut wrong = holds.clone();
        wrong.retain(|t| !t.to_string().contains("extra"));
        let err = settle_release(&History::embedded(), &pool, &[&GUARDED_V2], &[], &wrong, &release(false))
            .await
            .expect_err("the shape differs")
            .to_string();
        assert!(err.contains("guard_probe") && err.contains("extra"), "{err}");
        assert_eq!(recorded_ids(&pool).await, vec![DRAFT_EXTRA[0].id.to_string()], "rolled back");
    }

    /// The live database is a running install: a release that cannot get
    /// a table lock within its lock wait gives way, so the install's
    /// writes queued behind its request go through, then tries again and
    /// lands once the table is free.
    #[sqlx::test]
    async fn a_release_gives_way_to_a_held_table_instead_of_stalling_its_writes(pool: PgPool) {
        apply(&pool, &[&GUARDED], NONE).await.expect("origin");
        sqlx::raw_sql(ADD_EXTRA[0].sql).execute(&pool).await.unwrap();
        let expected = read_schema(&pool).await.unwrap();
        sqlx::raw_sql("ALTER TABLE guard_probe DROP COLUMN extra").execute(&pool).await.unwrap();

        // A reader of the running install holds the table open.
        let mut reader = pool.begin().await.unwrap();
        sqlx::query("SELECT count(*) FROM guard_probe").execute(&mut *reader).await.unwrap();

        let install = pool.clone();
        let the_install_keeps_writing = async move {
            let asked = std::time::Instant::now();
            while !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'guard_probe'::regclass AND NOT granted)",
            )
            .fetch_one(&install)
            .await
            .unwrap()
            {
                assert!(asked.elapsed() < std::time::Duration::from_secs(10), "the release never asked for the table");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            // This write queues behind the release's request for the
            // table, so it goes through only once the release gives way.
            tokio::time::timeout(
                std::time::Duration::from_secs(15),
                sqlx::query("INSERT INTO guard_probe (id) VALUES (1)").execute(&install),
            )
            .await
            .expect("the write sat behind the release past its lock wait")
            .unwrap();
            reader.commit().await.unwrap();
        };
        let settling = release(true);
        let groups = [&GUARDED_V2];
        let history = History::embedded();
        let (settled, ()) = tokio::join!(
            settle_release(&history, &pool, &groups, &[], &expected, &settling),
            the_install_keeps_writing,
        );
        settled.expect("the release ran once the table was free");
        assert!(has_extra_column(&pool).await, "the release ran");
        assert_eq!(recorded_ids(&pool).await, vec![ADD_EXTRA[0].id.to_string()]);
    }
}

/// A release only settles groups the live database has built; one the
/// code added since the last boot has no stamp and is left for the boot.
#[sqlx::test]
async fn a_group_the_database_never_built_is_unborn_to_a_release(pool: PgPool) {
    let mut conn = pool.acquire().await.unwrap();
    let all = weft_task_store::schema_guard::unborn_groups(&mut conn, &[&GUARDED, &OTHER]).await.unwrap();
    assert_eq!(all, vec!["guard_probe", "guard_other"], "no stamp table yet: every group is unborn");
    drop(conn);
    apply(&pool, &[&GUARDED], NONE).await.expect("build one group");
    let mut conn = pool.acquire().await.unwrap();
    let rest = weft_task_store::schema_guard::unborn_groups(&mut conn, &[&GUARDED, &OTHER]).await.unwrap();
    assert_eq!(rest, vec!["guard_other"]);
}

/// A database built before the migration history restarted has no record
/// of where the history starts, and is refused with the way out; one built
/// since carries it and boots again.
#[sqlx::test]
async fn a_database_from_before_the_history_is_refused(pool: PgPool) {
    apply(&pool, &[&GUARDED], NONE).await.expect("fresh apply");
    apply(&pool, &[&GUARDED], NONE).await.expect("a database built since boots again");

    sqlx::query("DELETE FROM weft_migration WHERE group_name = '_history'").execute(&pool).await.unwrap();
    let refused = apply(&pool, &[&GUARDED], NONE).await.expect_err("an older database is refused").to_string();
    assert!(refused.contains("scripts/scrub-old-install.sh"), "{refused}");
}

/// The boot's order with values that leave where they were: the expand
/// part makes where they go, a document's fill moves a value out of a
/// column that stays and a group's move copies a column that goes, and only
/// then does the contract part drop what they left.
mod expand_and_contract {
    use super::*;
    use serde_json::{json, Value};
    use weft_core::stored::{Kind, Op};
    use weft_task_store::documents::{Document, Encoding, Fill};
    use weft_task_store::schema_guard::apply_schema_with;
    use weft_task_store::{DataStep, Until};

    /// The shape before the move: each holder's units inside its row,
    /// and each plan's drive inside its JSON.
    static BEFORE: SchemaGroup = SchemaGroup {
        name: "moving_probe",
        tables: &["moving_holder", "moving_plan"],
        ddl: &[
            "CREATE TABLE IF NOT EXISTS moving_holder (id INT PRIMARY KEY, units JSONB NOT NULL DEFAULT '{}'::jsonb)",
            "CREATE TABLE IF NOT EXISTS moving_plan (id INT PRIMARY KEY, plan JSONB, plan_version INTEGER NOT NULL DEFAULT 1)",
        ],
        seed: &[],
        data_steps: &[],
    };

    /// Today's: the units are rows of their own, the drive a column of its
    /// own.
    static AFTER: SchemaGroup = SchemaGroup {
        name: "moving_probe",
        tables: &["moving_holder", "moving_plan", "moving_unit"],
        ddl: &[
            "CREATE TABLE IF NOT EXISTS moving_holder (id INT PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS moving_plan (id INT PRIMARY KEY, plan JSONB, \
             plan_version INTEGER NOT NULL DEFAULT 1, driver UUID)",
            "CREATE TABLE IF NOT EXISTS moving_unit (holder INT NOT NULL, name TEXT NOT NULL, n INT NOT NULL, \
             PRIMARY KEY (holder, name))",
        ],
        seed: &[],
        data_steps: &[DataStep {
            table: "moving_holder",
            until: Until::Dropped("units"),
            sql: "INSERT INTO moving_unit (holder, name, n) SELECT h.id, u.key, (u.value->>'n')::int \
                  FROM moving_holder h CROSS JOIN LATERAL jsonb_each(h.units) AS u ON CONFLICT DO NOTHING",
        }],
    };

    /// The release between them, as the generator splits it.
    static RELEASE: &[Migration] = &[
        Migration {
            group: "moving_probe",
            id: "20261023T120000_moving_probe_move",
            part: Part::Expand,
            draft: false,
            sql: "CREATE TABLE moving_unit (holder INT NOT NULL, name TEXT NOT NULL, n INT NOT NULL, PRIMARY KEY (holder, name));\n\
                  ALTER TABLE moving_plan ADD COLUMN driver UUID;",
        },
        Migration {
            group: "moving_probe",
            id: "20261023T120000_moving_probe_move.contract",
            part: Part::Contract,
            draft: false,
            sql: "ALTER TABLE moving_holder DROP COLUMN units;",
        },
    ];

    const PLAN: Kind<Value> = Kind::new("probe plan", &[&[Op::DropKey { at: "", key: "driver" }]]);

    static PLANS: Document = Document::of(
        &PLAN,
        "moving_plan",
        "plan",
        Encoding::Json,
        &[Fill { from: 1, sql: "UPDATE moving_plan SET driver = (plan->>'driver')::uuid WHERE plan_version = 1 AND plan ? 'driver'" }],
        None,
    );

    const DRIVER: &str = "0192f6c4-3a2b-7c3d-8e4f-5a6b7c8d9e0f";

    #[sqlx::test]
    async fn a_moved_value_lands_before_its_column_goes(pool: PgPool) {
        apply(&pool, &[&BEFORE], NONE).await.expect("the shape before the move");
        sqlx::query("INSERT INTO moving_holder (id, units) VALUES (1, $1)")
            .bind(json!({ "db": { "n": 2 }, "cache": { "n": 1 } }))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO moving_plan (id, plan) VALUES (1, $1)")
            .bind(json!({ "driver": DRIVER, "step": 3 }))
            .execute(&pool)
            .await
            .unwrap();

        apply_schema_with(&pool, &[&AFTER], &[&PLANS], RELEASE).await.expect("the boot of today's weft");

        let units: Vec<(i32, String, i32)> =
            sqlx::query_as("SELECT holder, name, n FROM moving_unit ORDER BY name").fetch_all(&pool).await.unwrap();
        assert_eq!(units, vec![(1, "cache".into(), 1), (1, "db".into(), 2)], "moved before the column went");
        let gone: Option<String> = sqlx::query_scalar(
            "SELECT column_name::text FROM information_schema.columns WHERE table_name = 'moving_holder' AND column_name = 'units'",
        )
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(gone, None, "the contract part dropped the column");
        let (plan, version, driver): (Value, i32, Option<uuid::Uuid>) =
            sqlx::query_as("SELECT plan, plan_version, driver FROM moving_plan WHERE id = 1").fetch_one(&pool).await.unwrap();
        assert_eq!((plan, version, driver.map(|d| d.to_string())), (json!({ "step": 3 }), 2, Some(DRIVER.to_string())));

        // A second boot has nothing left to do: the move's column is gone.
        apply_schema_with(&pool, &[&AFTER], &[&PLANS], RELEASE).await.expect("a boot with nothing to do");
        let units: i64 = sqlx::query_scalar("SELECT count(*) FROM moving_unit").fetch_one(&pool).await.unwrap();
        assert_eq!(units, 2);
    }

    /// A move runs on every boot until its column is dropped, so one that
    /// meets units it copied already lands each of them once.
    #[sqlx::test]
    async fn a_move_over_units_it_copied_already_lands_each_once(pool: PgPool) {
        apply(&pool, &[&BEFORE], NONE).await.expect("the shape before the move");
        sqlx::query("INSERT INTO moving_holder (id, units) VALUES (1, $1)")
            .bind(json!({ "db": { "n": 2 }, "cache": { "n": 1 } }))
            .execute(&pool)
            .await
            .unwrap();
        apply_schema_with(&pool, &[&AFTER], &[&PLANS], &RELEASE[..1]).await.expect_err(
            "the release without its contract part: the shape check refuses it, and the move rolls back with it",
        );
        let copied: i64 = sqlx::query_scalar("SELECT count(*) FROM moving_unit").fetch_one(&pool).await.unwrap();
        assert_eq!(copied, 0, "the move runs in the contract transaction, and rolled back with it");
        // One unit already where it lives now, as a move that ran before
        // its column's drop shipped leaves it.
        sqlx::query("INSERT INTO moving_unit (holder, name, n) VALUES (1, 'db', 2)").execute(&pool).await.unwrap();
        apply_schema_with(&pool, &[&AFTER], &[&PLANS], RELEASE).await.expect("the boot of today's weft");
        let units: Vec<(i32, String, i32)> =
            sqlx::query_as("SELECT holder, name, n FROM moving_unit ORDER BY name").fetch_all(&pool).await.unwrap();
        assert_eq!(units, vec![(1, "cache".into(), 1), (1, "db".into(), 2)]);
    }

    /// A fresh database is built from today's DDL, which has no column to
    /// move out of: the boot has nothing to move, and its documents are
    /// recorded at today's version.
    #[sqlx::test]
    async fn a_fresh_database_never_had_the_moved_column(pool: PgPool) {
        apply_schema_with(&pool, &[&AFTER], &[&PLANS], RELEASE).await.expect("a fresh boot");
        let recorded: Vec<(String, i32)> =
            sqlx::query_as("SELECT column_name, version FROM weft_document_version ORDER BY column_name")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(recorded, vec![("plan".into(), 2)]);
    }

    /// A column the code still reads, missing from its table, is a
    /// mistake, said as one.
    #[sqlx::test]
    async fn a_live_column_missing_from_its_table_is_refused(pool: PgPool) {
        static MISSING: Document = Document::of(&PLAN, "moving_holder", "plan", Encoding::Json, &[], None);
        let err = format!(
            "{:#}",
            apply_schema_with(&pool, &[&AFTER], &[&MISSING], RELEASE).await.expect_err("refused")
        );
        assert!(err.contains("moving_holder.plan") && err.contains("DataStep"), "{err}");
    }
}

/// A column added NOT NULL to a table that holds rows, filled from another
/// table by a data step (`Until::NotNull`) between the expand part, which
/// adds it nullable, and the contract part, which makes it NOT NULL: the
/// rows take their value, a row with none to take is dropped, and the
/// step stops once the column is NOT NULL.
mod filled_before_not_null {
    use super::*;
    use weft_task_store::schema_guard::apply_schema_with;
    use weft_task_store::{DataStep, Until};

    static OWNERS: SchemaGroup = SchemaGroup {
        name: "fill_owner",
        tables: &["fill_owner"],
        ddl: &["CREATE TABLE IF NOT EXISTS fill_owner (id INT PRIMARY KEY, tenant TEXT NOT NULL)"],
        seed: &[],
        data_steps: &[],
    };

    static BEFORE: SchemaGroup = SchemaGroup {
        name: "fill_probe",
        tables: &["fill_probe"],
        ddl: &["CREATE TABLE IF NOT EXISTS fill_probe (id INT PRIMARY KEY)"],
        seed: &[],
        data_steps: &[],
    };

    /// Each row's tenant is its owner's; one with no owner is dropped.
    /// Counted in `fill_ran`, so a test sees whether the step ran.
    static AFTER: SchemaGroup = SchemaGroup {
        name: "fill_probe",
        tables: &["fill_probe"],
        ddl: &["CREATE TABLE IF NOT EXISTS fill_probe (id INT PRIMARY KEY, tenant TEXT NOT NULL)"],
        seed: &[],
        data_steps: &[DataStep {
            table: "fill_probe",
            until: Until::NotNull("tenant"),
            sql: "UPDATE fill_probe p SET tenant = o.tenant FROM fill_owner o WHERE o.id = p.id AND p.tenant IS NULL; \
                  DELETE FROM fill_probe WHERE tenant IS NULL; \
                  INSERT INTO fill_ran DEFAULT VALUES",
        }],
    };

    static RELEASE: &[Migration] = &[
        Migration {
            group: "fill_probe",
            id: "20261023T130000_fill_probe_tenant",
            part: Part::Expand,
            draft: false,
            sql: "ALTER TABLE fill_probe ADD COLUMN tenant TEXT;",
        },
        Migration {
            group: "fill_probe",
            id: "20261023T130000_fill_probe_tenant.contract",
            part: Part::Contract,
            draft: false,
            sql: "ALTER TABLE fill_probe ALTER COLUMN tenant SET NOT NULL;",
        },
    ];

    async fn ran(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM fill_ran").fetch_one(pool).await.unwrap()
    }

    #[sqlx::test]
    async fn the_rows_take_their_value_before_the_column_is_made_not_null(pool: PgPool) {
        apply(&pool, &[&OWNERS, &BEFORE], NONE).await.expect("the shape before the move");
        sqlx::raw_sql(
            "CREATE TABLE fill_ran (at TIMESTAMPTZ NOT NULL DEFAULT now()); \
             INSERT INTO fill_owner VALUES (1, 'acme'), (2, 'globex'); \
             INSERT INTO fill_probe VALUES (1), (2), (3)",
        )
        .execute(&pool)
        .await
        .unwrap();

        apply_schema_with(&pool, &[&OWNERS, &AFTER], &[], RELEASE).await.expect("the boot of today's weft");
        let rows: Vec<(i32, String)> =
            sqlx::query_as("SELECT id, tenant FROM fill_probe ORDER BY id").fetch_all(&pool).await.unwrap();
        assert_eq!(rows, vec![(1, "acme".into()), (2, "globex".into())], "filled, and the row with no owner dropped");
        sqlx::query("INSERT INTO fill_probe (id) VALUES (4)").execute(&pool).await.expect_err("the column is NOT NULL");
        assert_eq!(ran(&pool).await, 1);

        // The column is NOT NULL now: a boot with schema work to do runs
        // the step no more (a fingerprint moved by hand makes it work).
        sqlx::query("UPDATE weft_schema_stamp SET fingerprint = 'moved' WHERE group_name = 'fill_probe'")
            .execute(&pool)
            .await
            .unwrap();
        apply_schema_with(&pool, &[&OWNERS, &AFTER], &[], RELEASE).await.expect("a boot that restamps");
        assert_eq!(ran(&pool).await, 1, "the step stopped once the column was NOT NULL");
    }

    /// A fresh database is built with the column NOT NULL: nothing to fill.
    #[sqlx::test]
    async fn a_fresh_database_runs_no_step(pool: PgPool) {
        sqlx::raw_sql("CREATE TABLE fill_ran (at TIMESTAMPTZ NOT NULL DEFAULT now())").execute(&pool).await.unwrap();
        apply_schema_with(&pool, &[&OWNERS, &AFTER], &[], RELEASE).await.expect("a fresh boot");
        assert_eq!(ran(&pool).await, 0);
    }
}
