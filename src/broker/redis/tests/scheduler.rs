use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[test]
fn async_scheduler_run_once_enqueues_due_task_to_redis() {
    let Some(mut fixture) = RedisFixture::new("async-scheduler-run-once") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_scheduler_run_once_enqueues_due_task_to_redis_inner(&mut fixture),
    );
}

async fn async_scheduler_run_once_enqueues_due_task_to_redis_inner(fixture: &mut RedisFixture) {
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let due = base + Duration::from_secs(1);
    let task_id = "entry-id:1700000001";
    let broker = fixture.async_broker().await;
    let mut scheduler =
        Scheduler::with_clock("scheduler-id", broker, StepClock::new(base, due)).unwrap();
    scheduler
        .register_with(
            "entry-id",
            Task::new("email:scheduled", b"payload".to_vec()),
            Duration::from_secs(1),
            EnqueueOptions::new().queue(crate::QueueName::new(fixture.queue()).unwrap()),
        )
        .unwrap();

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert_eq!(pending_ids, [task_id]);
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key(task_id))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    let message = decode_msg(stored.get("msg").unwrap());
    assert_eq!(message.id, task_id);
    assert_eq!(message.r#type, "email:scheduled");
    assert_eq!(message.queue, fixture.queue());

    let metadata: Vec<Vec<u8>> = fixture
        .connection
        .lrange("asynq:schedulers:{scheduler-id}", 0, -1)
        .unwrap();
    assert_eq!(metadata.len(), 1);
    let entry = pb::asynq::SchedulerEntry::decode(metadata[0].as_slice()).unwrap();
    assert_eq!(entry.id, "entry-id");
    assert_eq!(entry.spec, "@every 1s");
    assert_eq!(entry.task_type, "email:scheduled");

    let events: Vec<Vec<u8>> = fixture
        .connection
        .zrange("asynq:scheduler_history:entry-id", 0, -1)
        .unwrap();
    assert_eq!(events.len(), 1);
    let event = pb::asynq::SchedulerEnqueueEvent::decode(events[0].as_slice()).unwrap();
    assert_eq!(event.task_id, task_id);
}

#[derive(Clone, Debug)]
struct StepClock {
    first: SystemTime,
    later: SystemTime,
    calls: Arc<AtomicUsize>,
}

impl StepClock {
    fn new(first: SystemTime, later: SystemTime) -> Self {
        Self {
            first,
            later,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Clock for StepClock {
    fn now(&self) -> SystemTime {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first
        } else {
            self.later
        }
    }
}
