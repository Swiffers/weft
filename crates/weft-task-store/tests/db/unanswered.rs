//! weft's own work that keeps failing, against a real Postgres: what a
//! project's status is shown is what the SQL keeps and reads back.

use sqlx::PgPool;
use uuid::Uuid;

use weft_task_store::unanswered::{self, Callee, Failed};

/// The schema these read, with the few columns of the journal's `run`
/// table a project's workers' failures are judged by.
async fn schema(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&unanswered::GROUP]).await.expect("schema");
    unanswered::judged_run_columns(pool).await.expect("the run columns");
}

/// A failing role or loop shows for every project, a project's workers
/// and infra only for that project; a second failure keeps when the
/// stretch began and takes the newest error; an answer clears it; and a
/// failure stays current however old it is.
#[sqlx::test]
async fn a_failure_is_shown_to_the_projects_it_holds_up_until_it_is_resolved(pool: PgPool) {
    schema(&pool).await;
    let (mine, theirs) = (Uuid::from_u128(1), Uuid::from_u128(2));

    unanswered::failed(&pool, Callee::Role("supervisor"), None, "411 Length Required").await.unwrap().log();
    unanswered::failed(&pool, Callee::Workers(theirs), None, "connection refused").await.unwrap().log();
    unanswered::failed(&pool, Callee::Infra(theirs), None, "quota").await.unwrap().log();
    unanswered::failed(&pool, Callee::Loop { role: "dispatcher", name: "delivery" }, Some("hand queued runs to workers"), "boom").await.unwrap().log();
    let shown = unanswered::failing_for(&pool, mine).await.unwrap();
    let what: Vec<&Failed> = shown.iter().map(|f| &f.what).collect();
    assert_eq!(
        what,
        vec![&Failed::Loop { role: "dispatcher".into(), name: "delivery".into() }, &Failed::Role("supervisor".into())],
        "another project's workers and infra are not mine"
    );
    assert_eq!(shown[0].label.as_deref(), Some("hand queued runs to workers"));

    let workers_of = async |project| unanswered::failing_for(&pool, project).await.unwrap().into_iter().find(|f| f.what == Failed::Workers);
    unanswered::failed(&pool, Callee::Workers(mine), None, "first").await.unwrap().log();
    let since = workers_of(mine).await.unwrap().since_ms;
    unanswered::failed(&pool, Callee::Workers(mine), None, "second").await.unwrap().log();
    let workers = workers_of(mine).await.unwrap();
    assert_eq!((workers.error.as_str(), workers.since_ms), ("second", since), "the stretch keeps its start");

    unanswered::answered(&pool, Callee::Role("supervisor")).await.unwrap();
    let shown = unanswered::failing_for(&pool, mine).await.unwrap();
    assert!(shown.iter().all(|f| f.what != Failed::Role("supervisor".into())), "an answered ring is cleared: {shown:?}");

    sqlx::query("UPDATE unanswered_call SET since_ms = since_ms - 3600000, last_ms = last_ms - 3600000").execute(&pool).await.unwrap();
    let shown = unanswered::failing_for(&pool, mine).await.unwrap();
    assert_eq!(shown.len(), 2, "an hour-old failure nobody resolved is still current: {shown:?}");
}

/// A repair that failed is shown to its own project only, and a look that
/// repaired everything of a project clears it, leaving the projects still
/// failing shown; a look of another kind (signals kept past their door's
/// window) clears only its own kind.
#[sqlx::test]
async fn a_failed_repair_is_shown_to_its_project_until_a_look_repairs_it(pool: PgPool) {
    use unanswered::PerProject;
    schema(&pool).await;
    let (mine, theirs) = (Uuid::from_u128(1), Uuid::from_u128(2));
    unanswered::failed(&pool, Callee::Repairs(mine), None, "run 7 could not be let go of").await.unwrap().log();
    unanswered::failed(&pool, Callee::Repairs(theirs), None, "a resync of theirs").await.unwrap().log();
    unanswered::failed(&pool, Callee::LetGo(mine), None, "node poll: refused").await.unwrap().log();
    let of = async |project, what: Failed| {
        unanswered::failing_for(&pool, project).await.unwrap().into_iter().filter(|f| f.what == what).map(|f| f.error).collect::<Vec<_>>()
    };
    assert_eq!(of(mine, Failed::Repairs).await, ["run 7 could not be let go of"], "nobody else's repairs are shown");
    unanswered::still_failing(&pool, PerProject::Repairs, &[theirs]).await.unwrap();
    assert!(of(mine, Failed::Repairs).await.is_empty(), "repaired");
    assert_eq!(of(theirs, Failed::Repairs).await, ["a resync of theirs"], "still failing");
    assert_eq!(of(mine, Failed::LetGo).await, ["node poll: refused"], "a look at repairs leaves other kinds alone");
    unanswered::still_failing(&pool, PerProject::Repairs, &[]).await.unwrap();
    assert!(of(theirs, Failed::Repairs).await.is_empty());
    unanswered::still_failing(&pool, PerProject::LetGo, &[]).await.unwrap();
    assert!(of(mine, Failed::LetGo).await.is_empty());
}

/// A project removed takes every failure that holds it up with it, and
/// leaves another project's, and the ones that hold up every project.
#[sqlx::test]
async fn a_project_removed_forgets_exactly_its_own_failures(pool: PgPool) {
    schema(&pool).await;
    let (gone, kept) = (Uuid::from_u128(1), Uuid::from_u128(2));
    for project in [gone, kept] {
        for callee in [Callee::Workers(project), Callee::Infra(project), Callee::Commands(project), Callee::Repairs(project), Callee::LetGo(project)] {
            unanswered::failed(&pool, callee, None, "boom").await.unwrap().log();
        }
    }
    unanswered::failed(&pool, Callee::Role("supervisor"), None, "503").await.unwrap().log();
    unanswered::failed(&pool, Callee::GoneCopies, None, "denied").await.unwrap().log();
    unanswered::forget_project(&pool, gone).await.unwrap();
    let left: Vec<String> = sqlx::query_scalar("SELECT callee FROM unanswered_call ORDER BY callee").fetch_all(&pool).await.unwrap();
    assert_eq!(
        left,
        [
            format!("commands:{kept}"),
            "gone-copies:".to_string(),
            format!("infra:{kept}"),
            format!("let-go:{kept}"),
            format!("repairs:{kept}"),
            "role:supervisor".to_string(),
            format!("workers:{kept}"),
        ]
    );
}

/// An error holding a NUL byte, which a Postgres text column refuses, is
/// still written down, without the byte.
#[sqlx::test]
async fn an_error_with_a_nul_byte_is_still_written_down(pool: PgPool) {
    schema(&pool).await;
    let project = Uuid::from_u128(1);
    unanswered::failed(&pool, Callee::Infra(project), None, "bad\0byte").await.unwrap().log();
    let shown = unanswered::failing_for(&pool, project).await.unwrap();
    assert_eq!(shown.iter().map(|f| f.error.as_str()).collect::<Vec<_>>(), ["badbyte"]);
}

/// A project's workers stop being shown as failing once a run of the
/// project born after the failure reached a worker, with no write on the
/// way; a run still queued does not count, nor one born before, and a
/// queued run its workers answered busy keeps it shown. A failure after
/// that starts a new stretch.
#[sqlx::test]
async fn a_workers_failure_ends_once_a_later_run_reached_a_worker(pool: PgPool) {
    schema(&pool).await;
    let project = Uuid::from_u128(1);
    let failing = async || unanswered::failing_for(&pool, project).await.unwrap().into_iter().find(|f| f.what == Failed::Workers);
    unanswered::failed(&pool, Callee::Workers(project), None, "connection refused").await.unwrap().log();
    let first = failing().await.expect("current");
    let run = |state: &'static str, started_at: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query("INSERT INTO run (run_id, project_id, state, started_at) VALUES ($1, $2, $3, $4)")
                .bind(Uuid::new_v4())
                .bind(project)
                .bind(state)
                .bind(started_at)
                .execute(&pool)
                .await
                .unwrap();
        }
    };
    run("running", first.last_ms / 1000 - 60).await;
    run("queued", first.last_ms / 1000 + 60).await;
    assert!(failing().await.is_some(), "a run born before, or one still queued, says nothing");
    run("running", first.last_ms / 1000 + 60).await;
    assert!(failing().await.is_none(), "a later run reached a worker");
    sqlx::query("UPDATE run SET busy_since = 1 WHERE state = 'queued'").execute(&pool).await.unwrap();
    assert!(failing().await.is_some(), "a queued run its workers answered busy still waits on them");
    sqlx::query("UPDATE run SET busy_since = NULL, unreached = 1 WHERE state = 'queued'").execute(&pool).await.unwrap();
    assert!(failing().await.is_some(), "a queued run they could not be reached with still waits on them");
    sqlx::query("UPDATE run SET unreached = 0, refusals = 1 WHERE state = 'queued'").execute(&pool).await.unwrap();
    assert!(failing().await.is_some(), "a queued run they refused still waits on them");
    sqlx::query("UPDATE run SET refusals = 0").execute(&pool).await.unwrap();
    sqlx::query("UPDATE unanswered_call SET last_ms = last_ms + 120000").execute(&pool).await.unwrap();
    unanswered::failed(&pool, Callee::Workers(project), None, "again").await.unwrap().log();
    let since: i64 = sqlx::query_scalar("SELECT since_ms FROM unanswered_call").fetch_one(&pool).await.unwrap();
    assert_eq!(since, first.since_ms, "no run reached a worker after the last failure: the same stretch");
    sqlx::query("UPDATE unanswered_call SET last_ms = $1").bind(first.last_ms).execute(&pool).await.unwrap();
    unanswered::failed(&pool, Callee::Workers(project), None, "again").await.unwrap().log();
    let since: i64 = sqlx::query_scalar("SELECT since_ms FROM unanswered_call").fetch_one(&pool).await.unwrap();
    assert!(since > first.since_ms, "a run reached a worker since the last failure: a new stretch");
}

/// Counts the alert lines logged (`weft_platform_traits::ALERT_TARGET`).
#[derive(Clone, Default)]
struct AlertLines(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AlertLines {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == weft_platform_traits::ALERT_TARGET {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// On an install that alerts, a failure weft gave up retrying alerts at
/// its first write and never again in its stretch; one weft still retries
/// alerts on the first write past the install's delay; one that cleared
/// and came back alerts again; and an install that does not alert logs
/// nothing.
#[sqlx::test]
async fn a_stuck_failure_alerts_once_per_stretch_where_the_install_alerts(pool: PgPool) {
    use tracing_subscriber::layer::SubscriberExt as _;
    schema(&pool).await;
    let lines = AlertLines::default();
    let _logging = tracing::subscriber::set_default(tracing_subscriber::registry().with(lines.clone()));
    let logged = || lines.0.load(std::sync::atomic::Ordering::SeqCst);
    let alerted = async |callee: &str| -> Option<i64> {
        sqlx::query_scalar("SELECT alerted_ms FROM unanswered_call WHERE callee = $1").bind(callee).fetch_one(&pool).await.unwrap()
    };
    let project = Uuid::from_u128(1);
    let infra = format!("infra:{project}");

    unanswered::gave_up(&pool, Callee::Infra(project), None, "quota").await.unwrap().log();
    assert_eq!((alerted(&infra).await, logged()), (None, 0), "an install that does not alert logs nothing");
    unanswered::answered(&pool, Callee::Infra(project)).await.unwrap();

    unanswered::alert_after(&pool, Some(std::time::Duration::from_secs(3600))).await.unwrap();
    unanswered::alert_after(&pool, Some(std::time::Duration::from_secs(3600))).await.unwrap();
    unanswered::gave_up(&pool, Callee::Infra(project), None, "quota").await.unwrap().log();
    let first = alerted(&infra).await.expect("given up: alerted at once");
    assert_eq!(logged(), 1);
    unanswered::gave_up(&pool, Callee::Infra(project), None, "quota again").await.unwrap().log();
    assert_eq!((alerted(&infra).await, logged()), (Some(first), 1), "once per stretch");

    unanswered::failed(&pool, Callee::Role("supervisor"), None, "503").await.unwrap().log();
    unanswered::failed(&pool, Callee::Role("supervisor"), None, "503").await.unwrap().log();
    assert_eq!((alerted("role:supervisor").await, logged()), (None, 1), "still retried, and inside the delay");
    sqlx::query("UPDATE unanswered_call SET since_ms = since_ms - 7200000 WHERE callee = 'role:supervisor'").execute(&pool).await.unwrap();
    unanswered::failed(&pool, Callee::Role("supervisor"), None, "503").await.unwrap().log();
    assert!(alerted("role:supervisor").await.is_some(), "past the delay");
    assert_eq!(logged(), 2);

    unanswered::answered(&pool, Callee::Infra(project)).await.unwrap();
    unanswered::gave_up(&pool, Callee::Infra(project), None, "quota").await.unwrap().log();
    assert_eq!(logged(), 3, "cleared and back: a new stretch alerts again");

    // A failure first written once it had lasted the delay already began
    // that long ago, so it alerts at that first write.
    let workers = format!("workers:{project}");
    let hour = std::time::Duration::from_secs(3600);
    unanswered::failed_for(&pool, Callee::Workers(project), None, "busy for an hour", hour).await.unwrap().log();
    let (since, last): (i64, i64) =
        sqlx::query_as("SELECT since_ms, last_ms FROM unanswered_call WHERE callee = $1").bind(&workers).fetch_one(&pool).await.unwrap();
    assert_eq!(last - since, 3_600_000, "the stretch began when the failure did");
    assert_eq!((alerted(&workers).await, logged()), (Some(last), 4), "past the delay at its first write");

    unanswered::alert_after(&pool, None).await.unwrap();
    unanswered::gave_up(&pool, Callee::News, None, "the bus is away").await.unwrap().log();
    assert_eq!((alerted("news:").await, logged()), (None, 4), "alerts turned off");
}

/// A write that raised an alert holds it until its caller logs it: one
/// rolled back raised nothing that stays, and the next write that commits
/// raises it.
#[sqlx::test]
async fn an_alert_is_raised_only_by_a_write_that_commits(pool: PgPool) {
    schema(&pool).await;
    unanswered::alert_after(&pool, Some(std::time::Duration::from_secs(3600))).await.unwrap();
    let project = Uuid::from_u128(1);
    let mut tx = pool.begin().await.unwrap();
    let rolled_back = unanswered::gave_up(&mut *tx, Callee::Infra(project), None, "quota").await.unwrap();
    tx.rollback().await.unwrap();
    // Rolled back: its alert is dropped unlogged, which is the point.
    drop(rolled_back);
    let mut tx = pool.begin().await.unwrap();
    let committed = unanswered::gave_up(&mut *tx, Callee::Infra(project), None, "quota").await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(committed.len(), 1, "the write that commits raises it: {committed:?}");
    committed.log();
}

/// Every write of a row moves its last time strictly forward, even when it
/// finds a later time there (a write that waited on the row's lock), so
/// only the write that set the alert mark answers that it raised it.
#[sqlx::test]
async fn a_rows_last_write_only_moves_forward_and_raises_its_alert_once(pool: PgPool) {
    schema(&pool).await;
    unanswered::alert_after(&pool, Some(std::time::Duration::from_secs(3600))).await.unwrap();
    let project = Uuid::from_u128(1);
    unanswered::failed(&pool, Callee::Workers(project), None, "first").await.unwrap().log();
    sqlx::query("UPDATE unanswered_call SET last_ms = last_ms + 600000, since_ms = since_ms - 7200000").execute(&pool).await.unwrap();
    let ahead: i64 = sqlx::query_scalar("SELECT last_ms FROM unanswered_call").fetch_one(&pool).await.unwrap();
    let raised = unanswered::failed(&pool, Callee::Workers(project), None, "second").await.unwrap();
    let (last, alerted): (i64, Option<i64>) =
        sqlx::query_as("SELECT last_ms, alerted_ms FROM unanswered_call").fetch_one(&pool).await.unwrap();
    assert_eq!(last, ahead + 1, "never back, always forward");
    assert_eq!(alerted, Some(last), "the alert mark takes the write's own time");
    assert_eq!(raised.len(), 1, "{raised:?}");
    raised.log();
    let again = unanswered::failed(&pool, Callee::Workers(project), None, "third").await.unwrap();
    assert!(again.is_empty(), "raised once per stretch: {again:?}");
    again.log();
}

/// A ring given up on stays written down however old it is, so the next
/// process that starts rings that role again; a project's workers and a
/// loop are not roles and are never in that list.
#[sqlx::test]
async fn a_ring_given_up_on_is_found_again_at_the_next_start(pool: PgPool) {
    schema(&pool).await;
    unanswered::failed(&pool, Callee::Role("supervisor"), None, "503").await.unwrap().log();
    unanswered::failed(&pool, Callee::Workers(Uuid::from_u128(1)), None, "connection refused").await.unwrap().log();
    unanswered::failed(&pool, Callee::Loop { role: "broker", name: "runtime_file_expiry" }, Some("expire stored files"), "boom").await.unwrap().log();
    assert_eq!(unanswered::unanswered_roles(&pool).await.unwrap(), vec!["supervisor".to_string()]);
    unanswered::answered(&pool, Callee::Role("supervisor")).await.unwrap();
    assert!(unanswered::unanswered_roles(&pool).await.unwrap().is_empty());
}
