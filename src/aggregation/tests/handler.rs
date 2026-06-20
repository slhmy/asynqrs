use super::*;

#[tokio::test]
async fn group_aggregator_handler_builds_aggregate_task_from_set_messages() {
    type AggregatedTaskCall = (String, Vec<u8>, Option<String>);

    #[derive(Debug, Default)]
    struct RecordingGroupAggregator {
        calls: Vec<(String, Vec<AggregatedTaskCall>)>,
    }

    #[async_trait]
    impl GroupAggregator for RecordingGroupAggregator {
        async fn aggregate(
            &mut self,
            group: &str,
            tasks: Vec<Task>,
        ) -> Result<Task, AggregationError> {
            self.calls.push((
                group.to_owned(),
                tasks
                    .iter()
                    .map(|task| {
                        (
                            task.type_name().to_owned(),
                            task.payload().to_vec(),
                            task.header("trace-id").map(str::to_owned),
                        )
                    })
                    .collect(),
            ));
            Ok(Task::new("email:batch", b"aggregated".to_vec()))
        }
    }

    let mut message = TaskMessage::from_task(&Task::with_headers(
        "email:welcome",
        b"payload".to_vec(),
        [("trace-id", "abc")],
    ));
    message.group_key = "tenant-a".to_owned();
    message.queue = "critical".to_owned();
    let mut handler = GroupAggregatorHandler::new(RecordingGroupAggregator::default());

    let task = handler
        .handle_aggregation(
            "critical",
            "tenant-a",
            "set-id",
            AggregationSet::new(vec![message], SystemTime::UNIX_EPOCH),
        )
        .await
        .unwrap();

    assert_eq!(
        handler.aggregator().calls,
        [(
            "tenant-a".to_owned(),
            vec![(
                "email:welcome".to_owned(),
                b"payload".to_vec(),
                Some("abc".to_owned()),
            )]
        )]
    );
    assert_eq!(task.type_name(), "email:batch");
    assert_eq!(task.payload(), b"aggregated");
}

#[tokio::test]
async fn group_aggregator_func_adapter_aggregates_tasks_like_upstream() {
    let mut aggregator = GroupAggregatorFunc(|group: &str, tasks: Vec<Task>| {
        Task::new(
            format!("email:batch:{group}"),
            tasks
                .iter()
                .map(|task| task.payload().len() as u8)
                .collect::<Vec<_>>(),
        )
    });

    let task = GroupAggregator::aggregate(
        &mut aggregator,
        "tenant-a",
        vec![
            Task::new("email:welcome", b"one".to_vec()),
            Task::new("email:reminder", b"three".to_vec()),
        ],
    )
    .await
    .unwrap();

    assert_eq!(task.type_name(), "email:batch:tenant-a");
    assert_eq!(task.payload(), &[3, 5]);
}

#[test]
fn group_aggregator_func_inherent_aggregate_method_matches_upstream_name() {
    let mut aggregator = GroupAggregatorFunc(|group: &str, tasks: Vec<Task>| {
        Task::new(format!("email:batch:{group}"), vec![tasks.len() as u8])
    });

    let task = aggregator.aggregate(
        "tenant-a",
        vec![
            Task::new("email:welcome", b"one".to_vec()),
            Task::new("email:reminder", b"three".to_vec()),
        ],
    );

    assert_eq!(task.type_name(), "email:batch:tenant-a");
    assert_eq!(task.payload(), &[2]);
}
