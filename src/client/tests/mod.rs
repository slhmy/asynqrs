use super::*;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{
    EnqueueOptions, EnqueuePlan, EnqueuePlanError, MakeRedisClientError, RedisRuntimeClient, Task,
    TaskState,
};

mod async_enqueue;
mod lifecycle;
mod sync_enqueue;

#[derive(Debug, Default)]
struct RecordingBroker {
    plans: Vec<EnqueuePlan>,
    error: Option<BrokerError>,
    ping_error: Option<BrokerError>,
    close_error: Option<BrokerError>,
    ping_calls: usize,
    close_calls: usize,
}

impl Broker for RecordingBroker {
    fn ping(&mut self) -> Result<(), BrokerError> {
        self.ping_calls += 1;
        if let Some(error) = self.ping_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        self.plans.push(plan.clone());
        Ok(())
    }
}

impl CloseBroker for RecordingBroker {
    fn close(&mut self) -> Result<(), BrokerError> {
        self.close_calls += 1;
        if let Some(error) = self.close_error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncBroker for RecordingBroker {
    async fn ping(&mut self) -> Result<(), BrokerError> {
        Broker::ping(self)
    }

    async fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        Broker::enqueue(self, plan)
    }
}

#[derive(Debug)]
struct FixedTaskIdGenerator(&'static str);

impl TaskIdGenerator for FixedTaskIdGenerator {
    fn generate_task_id(&mut self) -> String {
        self.0.to_owned()
    }
}

#[derive(Debug, Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug)]
struct AdvancingClock {
    first: SystemTime,
    step: Duration,
    calls: Cell<u32>,
}

impl AdvancingClock {
    fn new(first: SystemTime, step: Duration) -> Self {
        Self {
            first,
            step,
            calls: Cell::new(0),
        }
    }
}

impl Clock for AdvancingClock {
    fn now(&self) -> SystemTime {
        let calls = self.calls.get();
        self.calls.set(calls + 1);
        self.first + self.step * calls
    }
}

#[derive(Debug)]
struct GenerationAwareClock {
    first: SystemTime,
    generated: Rc<Cell<bool>>,
}

impl Clock for GenerationAwareClock {
    fn now(&self) -> SystemTime {
        if self.generated.get() {
            self.first + Duration::from_nanos(10)
        } else {
            self.first
        }
    }
}

#[derive(Debug)]
struct MarkingTaskIdGenerator {
    generated: Rc<Cell<bool>>,
}

impl TaskIdGenerator for MarkingTaskIdGenerator {
    fn generate_task_id(&mut self) -> String {
        self.generated.set(true);
        "task-id".to_owned()
    }
}

#[derive(Debug)]
struct CountingTaskIdGenerator {
    calls: Rc<Cell<u32>>,
}

impl TaskIdGenerator for CountingTaskIdGenerator {
    fn generate_task_id(&mut self) -> String {
        self.calls.set(self.calls.get() + 1);
        "task-id".to_owned()
    }
}

#[test]
fn redis_backed_client_from_redis_client_matches_constructor_shape() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<RedisBackedClient, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(RedisBackedClient::from_redis_client(redis_client));
}

#[test]
fn client_new_keeps_custom_broker_constructor() {
    let mut client = Client::new(RecordingBroker::default());

    client.ping().unwrap();

    assert_eq!(client.broker.ping_calls, 1);
}

#[test]
fn redis_backed_client_from_runtime_client_matches_shared_constructor_shape() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<RedisBackedClient, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(RedisBackedClient::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
    ));
}

#[test]
fn redis_backed_client_from_direct_redis_client_keeps_direct_client_convenience() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<RedisBackedClient, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(RedisBackedClient::from_direct_redis_client(redis_client));
}

#[test]
fn redis_backed_client_from_redis_runtime_client_accepts_shared_runtime_boundary() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<RedisBackedClient, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(RedisBackedClient::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
    ));
}

#[test]
fn redis_backed_client_from_redis_client_accepts_redis_rs_client() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<RedisBackedClient, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(RedisBackedClient::from_redis_client(redis_client));
}

#[test]
fn client_error_sentinel_predicates_match_upstream_errors_is_checks() {
    let duplicate = ClientError::Broker(BrokerError::DuplicateTask);
    let conflict = ClientError::Broker(BrokerError::TaskIdConflict);
    let other = ClientError::Broker(BrokerError::Other("redis down".to_owned()));

    assert!(BrokerError::DuplicateTask.is_duplicate_task());
    assert!(!BrokerError::DuplicateTask.is_task_id_conflict());
    assert!(BrokerError::TaskIdConflict.is_task_id_conflict());
    assert!(!BrokerError::TaskIdConflict.is_duplicate_task());

    assert!(duplicate.is_duplicate_task());
    assert!(!duplicate.is_task_id_conflict());
    assert!(conflict.is_task_id_conflict());
    assert!(!conflict.is_duplicate_task());
    assert!(!other.is_duplicate_task());
    assert!(!other.is_task_id_conflict());
}

#[test]
fn public_client_error_sentinels_match_upstream_messages() {
    assert_eq!(
        BrokerError::DuplicateTask.to_string(),
        "task already exists"
    );
    assert_eq!(
        BrokerError::TaskIdConflict.to_string(),
        "task ID conflicts with another task"
    );
}
