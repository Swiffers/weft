    //! Layer 3: a charge an earlier drive of the run opened (a job started
    //! before the run paused or its worker died) is held again by the drive
    //! that picks the run up, and a run that then ends for good books it as
    //! unknown, exactly once, on the node that started the job. The
    //! pause-then-price path is pinned with the charges themselves
    //! (`crate::metering::open_charge_tests`).

    use super::*;
    use super::engine_test_rig::{catalog, drive_journal, test_manifest};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::Node;
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::ExecEvent;

    /// Does nothing: the run completes once it ran.
    struct Done;
    test_manifest!(Done, "Done");
    #[async_trait]
    impl Node for Done {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Ok(())
        }
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [{
                "id": "step", "nodeType": "Done", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                "inputs": [], "outputs": [],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": [],
            "groups": []
        }))
        .unwrap()
    }

    /// A run born and kicked, with a charge `step` opened on the meter the
    /// metering tests register (`queued`) and nothing closing it: the
    /// record an earlier drive left when it let go of the run.
    fn rows(project: &ProjectDefinition, execution_id: ExecutionId) -> Vec<ExecEvent> {
        vec![
            ExecEvent::ExecutionStarted {
                execution_id,
                project_id: project.id,
                entry_node: "step".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
                selection: None, seed: None, instance: None, stand_in: None, fired_trigger: None,
                instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            ExecEvent::NodeKicked {
                execution_id, node_id: "step".into(), frames: vec![], firing: false,
                payload: None, port_snapshot: None, at_unix: 0,
            },
            ExecEvent::ChargeOpened {
                execution_id,
                node_id: "step".into(),
                frames: vec![],
                service: "queued".into(),
                charge: "job-1".into(),
                scratch: json!({ "model": "m1" }),
                origin: weft_core::CredentialOwner::Author,
                at_unix: 0,
            },
        ]
    }

    /// The costs on the run's record: (node, amount, the charge closed).
    fn costs(events: &[ExecEvent]) -> Vec<(String, Option<f64>, Option<String>)> {
        events
            .iter()
            .filter_map(|event| match event {
                ExecEvent::CostReported { node_id, amount_usd, charge, .. } => Some((node_id.clone(), *amount_usd, charge.clone())),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_charge_left_open_on_the_record_is_booked_unknown_once_when_the_run_ends() {
        let project = project();
        let execution_id = uuid::Uuid::new_v4();
        let rows = rows(&project, execution_id);
        let (outcome, events) =
            drive_journal(project, catalog(vec![("Done", Box::new(Done))]), execution_id, rows, CancellationFlag::new_arc()).await;
        assert!(matches!(outcome.expect("the drive runs"), ExecutionOutcome::Completed));
        assert_eq!(costs(&events), vec![("step".to_string(), None, Some("job-1".to_string()))], "{events:?}");
        assert_eq!(weft_journal::ChargeBook::of_record(&events).into_open().count(), 0, "the record closed it");
    }

    /// A run cancelled while it was let go (a person stopping a run that
    /// waits) ends before anything of it runs: its open charge is booked
    /// all the same.
    #[tokio::test]
    async fn a_run_cancelled_before_it_was_picked_up_books_its_open_charge() {
        let project = project();
        let execution_id = uuid::Uuid::new_v4();
        // Without its kick: a run cancelled before it loads holds none of
        // its record's tables, and the rig checks the record folds back
        // into what the drive held.
        let mut rows = rows(&project, execution_id);
        rows.retain(|row| !matches!(row, ExecEvent::NodeKicked { .. }));
        let cancellation = CancellationFlag::new_arc();
        cancellation.cancel_because(weft_core::exec::CancelCause::User);
        let (outcome, events) =
            drive_journal(project, catalog(vec![("Done", Box::new(Done))]), execution_id, rows, cancellation).await;
        assert!(matches!(outcome.expect("the drive runs"), ExecutionOutcome::Cancelled { .. }));
        assert_eq!(costs(&events), vec![("step".to_string(), None, Some("job-1".to_string()))], "{events:?}");
    }
