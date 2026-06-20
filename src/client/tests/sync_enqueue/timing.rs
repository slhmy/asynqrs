use super::*;

#[test]
fn process_in_uses_client_clock() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new().process_in(Duration::from_secs(60)),
        )
        .unwrap();

    assert_eq!(result.state(), TaskState::Scheduled);
    assert_eq!(
        result.next_process_at(),
        Some(now + Duration::from_secs(60))
    );
    assert_eq!(client.broker.plans[0].state(), TaskState::Scheduled);
}

#[test]
fn process_in_due_by_second_client_clock_read_is_pending() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        AdvancingClock::new(now, Duration::from_nanos(1)),
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new().process_in(Duration::from_nanos(1)),
        )
        .unwrap();

    assert_eq!(result.state(), TaskState::Pending);
    assert_eq!(
        result.next_process_at(),
        Some(now + Duration::from_nanos(2))
    );
    assert_eq!(client.broker.plans[0].state(), TaskState::Pending);
}

#[test]
fn process_in_option_uses_composition_clock_read() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        AdvancingClock::new(now, Duration::from_secs(1)),
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new().process_in(Duration::from_secs(120)),
        )
        .unwrap();

    assert_eq!(result.state(), TaskState::Scheduled);
    assert_eq!(
        result.next_process_at(),
        Some(now + Duration::from_secs(121))
    );
}

#[test]
fn process_in_clock_read_happens_after_task_id_generation() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let generated = Rc::new(Cell::new(false));
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        MarkingTaskIdGenerator {
            generated: Rc::clone(&generated),
        },
        GenerationAwareClock {
            first: now,
            generated,
        },
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new().process_in(Duration::from_secs(60)),
        )
        .unwrap();

    assert_eq!(
        result.next_process_at(),
        Some(now + Duration::from_nanos(10) + Duration::from_secs(60))
    );
}
