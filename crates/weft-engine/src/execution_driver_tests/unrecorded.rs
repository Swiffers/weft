    //! Layer 3: an unrecorded run through the real loop. Its worker keeps
    //! no history of it: a run that completes writes nothing, one that
    //! fails writes its birth and its failure (marked as not recorded on
    //! its row), and a wait holds in its node's call, since there is
    //! nowhere to park it.

    use super::*;
    use super::engine_test_rig::{born, catalog, clients, run_on_holding, test_manifest, Answers, AwaitTasks, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::exec::RunKind;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::ExecEvent;

    struct Answer;
    test_manifest!(Answer, "Answer");
    #[async_trait]
    impl Node for Answer {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("ok"))).await
        }
    }

    struct Broken;
    test_manifest!(Broken, "Answer");
    #[async_trait]
    impl Node for Broken {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Err(weft_core::error::node_error("the status table is gone"))
        }
    }

    struct Waits;
    test_manifest!(Waits, "Answer");
    #[async_trait]
    impl Node for Waits {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 5_000 } };
            ctx.await_signal(timer).await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("late"))).await
        }
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "edges": [],
            "nodes": [{
                "id": "answer", "nodeType": "Answer", "position": {"x": 0, "y": 0},
                "outputs": [{"name": "value", "portType": "String", "required": false}]
            }]
        }))
        .unwrap()
    }

    /// Drive one unrecorded run of `node` the way the worker does. Answers
    /// the outcome and what the record holds of it once its writer is done.
    async fn drive_unrecorded(node: Box<dyn Node>) -> (ExecutionOutcome, Vec<ExecEvent>) {
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        drive_holding(node, weft_core::run_settings::DEFAULT_HOLD_SECS, AwaitTasks::new(), Arc::new(Answers::default()), &holds).await
    }

    /// [`drive_unrecorded`], the run holding its waits `hold_secs`, its
    /// waits registered with `tasks` and answered through `answers`, its
    /// quiet spells counted in `holds`.
    async fn drive_holding(
        node: Box<dyn Node>,
        hold_secs: u32,
        tasks: Arc<AwaitTasks>,
        answers: Arc<Answers>,
        holds: &Arc<crate::run_holds::Holds>,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        drive_run(weft_core::new_execution_id(), node, hold_secs, tasks, answers, holds).await
    }

    /// [`drive_holding`], of run `execution_id`.
    async fn drive_run(
        execution_id: weft_core::ExecutionId,
        node: Box<dyn Node>,
        hold_secs: u32,
        tasks: Arc<AwaitTasks>,
        answers: Arc<Answers>,
        holds: &Arc<crate::run_holds::Holds>,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        let project = project();
        let birth = vec![
            ExecEvent::ExecutionStarted {
                execution_id,
                project_id: project.id,
                entry_node: "answer".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
                binary_hash: None,
                source_version: None,
                run_kind: RunKind::Execution,
                selection: None,
                seed: None,
                instance: None,
                fired_trigger: None,
                stand_in: None,
                instance_values: Default::default(), picks: Default::default(),
                at_unix: 0,
                settings: weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Fast, false)
                    .and_then(|settings| settings.holding_for(hold_secs))
                    .unwrap(),
            },
            ExecEvent::NodeKicked { execution_id, node_id: "answer".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ];
        let journal = Arc::new(MemJournal::default());
        let run_clients = EngineClients { tasks, runs: answers, ..clients(journal.clone()) };
        let run = born(&run_clients, execution_id, &birth).await;
        let drove = run_on_holding(Arc::new(project), catalog(vec![("Answer", node)]), &run_clients, execution_id, run, birth, CancellationFlag::new_arc(), None, None, holds)
            .await
            .expect("the drive ends");
        run_clients.writer.written().await;
        (drove.outcome, journal.events_of(execution_id))
    }

    #[tokio::test]
    async fn a_completed_run_writes_nothing() {
        let (outcome, written) = drive_unrecorded(Box::new(Answer)).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(written.is_empty(), "nothing was written: {written:?}");
    }

    #[tokio::test]
    async fn a_failed_run_leaves_its_failure_and_nothing_else() {
        let (outcome, written) = drive_unrecorded(Box::new(Broken)).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        assert_eq!(written.len(), 2, "its birth and its failure: {written:?}");
        assert!(matches!(&written[0], ExecEvent::ExecutionStarted { settings, .. } if !settings.recorded()), "its row says it was not recorded: {written:?}");
        assert!(
            matches!(&written[1], ExecEvent::ExecutionFailed { error, .. } if error.contains("status table")),
            "the failure names what failed: {written:?}"
        );
    }

    /// A wait holds in its node's call, and its answer carries the run on
    /// in the same worker: nothing is written of it. While it only waited,
    /// the run counted as quiet (so its worker may stop ticking) until its
    /// hold would give up, and it moved again once answered.
    #[tokio::test]
    async fn a_wait_holds_and_takes_its_answer() {
        let (tasks, answers) = (AwaitTasks::new(), Arc::new(Answers::default()));
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        // Answered once the run is quiet, as a person answers a run that
        // waited, after reading how long it waits.
        let answering = tokio::spawn({
            let (tasks, answers, holds) = (tasks.clone(), answers.clone(), holds.clone());
            async move {
                let token = tasks.await_token().await;
                let latest = loop {
                    if let (1, Some(latest)) = holds.quiet() {
                        break latest;
                    }
                    tokio::task::yield_now().await;
                };
                answers.answer(token, json!("now"));
                latest
            }
        });
        let asked_at = crate::now_unix() as i64;
        let (outcome, written) = drive_holding(Box::new(Waits), 60, tasks.clone(), answers, &holds).await;
        let quiet_until = answering.await.unwrap();
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(written.is_empty(), "nothing was written: {written:?}");
        assert!(tasks.withdrawn.lock().unwrap().is_empty(), "an answered wait is not withdrawn");
        assert!((asked_at + 60..=crate::now_unix() as i64 + 61).contains(&quiet_until), "quiet until its hold gives up: {quiet_until}");
        assert_eq!(holds.quiet(), (0, None), "it moved again");
    }

    /// A waiting run reads its answers once as it starts waiting, and then
    /// only when its line tells it one was handed to it or may have missed
    /// some: another run's answer, the broker hearing again after a quiet
    /// spell, and the line lost (with no recheck yet) read nothing.
    #[tokio::test]
    async fn a_waiting_run_reads_its_answers_only_when_told() {
        let (tasks, answers) = (AwaitTasks::new(), Arc::new(Answers::default()));
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        let run = weft_core::new_execution_id();
        let telling = tokio::spawn({
            let (tasks, answers, holds) = (tasks.clone(), answers.clone(), holds.clone());
            async move {
                use weft_task_store::pg_signal::Heard;
                let token = tasks.await_token().await;
                while holds.quiet().0 == 0 {
                    tokio::task::yield_now().await;
                }
                let settled = || async {
                    for _ in 0..50 {
                        tokio::task::yield_now().await;
                    }
                };
                settled().await;
                assert_eq!(answers.reads(), 1, "read once as it started waiting");
                let named = |run| Heard::Signal { channel: weft_task_store::parked_fires::PARKED_FIRE_CHANNEL, payload: weft_task_store::runs::run_payload(uuid::Uuid::from_u128(9), run).into() };
                answers.line.send(named(weft_core::new_execution_id())).unwrap();
                answers.line.send(Heard::Resumed).unwrap();
                answers.line.send(Heard::Lost).unwrap();
                settled().await;
                assert_eq!(answers.reads(), 1, "nothing for this run: nothing read");
                answers.line.send(named(run)).unwrap();
                settled().await;
                assert_eq!(answers.reads(), 2, "told one was handed to it: one read");
                answers.answer(token, json!("now"));
            }
        });
        let (outcome, _) = drive_run(run, Box::new(Waits), 60, tasks, answers.clone(), &holds).await;
        telling.await.unwrap();
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(answers.reads(), 3, "the recheck its answer came with read it");
    }

    /// A wait nothing answers is given up once the run was quiet for its
    /// hold: the waiting call fails, naming why, and the wait is withdrawn.
    #[tokio::test]
    async fn a_wait_nothing_answers_is_given_up_after_its_hold() {
        let tasks = AwaitTasks::new();
        let (outcome, written) = drive_holding(Box::new(Waits), 1, tasks.clone(), Arc::new(Answers::default()), &crate::run_holds::Holds::new(Box::new(|| {}))).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let error = written
            .iter()
            .find_map(|e| match e {
                ExecEvent::ExecutionFailed { error, .. } => Some(error.clone()),
                _ => None,
            })
            .expect("the waiting node failed, so its run leaves its failure");
        assert!(error.contains("gave up its wait") && error.contains("`holdSecs`") && error.contains("`recorded: false`"), "{error}");
        assert_eq!(*tasks.withdrawn.lock().unwrap(), vec![tasks.minted().expect("the wait was registered")]);
        assert!(!written.iter().any(|e| matches!(e, ExecEvent::NodeSuspended { .. })), "nothing parked");
    }

    /// A hold of zero gives the wait up at once, through the one path a
    /// wait given up takes: registered, then given up and withdrawn.
    #[tokio::test]
    async fn a_hold_of_zero_fails_the_wait_at_once() {
        let tasks = AwaitTasks::new();
        let (outcome, _) = drive_holding(Box::new(Waits), 0, tasks.clone(), Arc::new(Answers::default()), &crate::run_holds::Holds::new(Box::new(|| {}))).await;
        assert!(matches!(&outcome, ExecutionOutcome::Failed { error } if error.contains("is 0")), "{outcome:?}");
        assert_eq!(*tasks.withdrawn.lock().unwrap(), vec![tasks.minted().expect("the wait was registered")]);
    }
