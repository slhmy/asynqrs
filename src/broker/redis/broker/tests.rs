use super::*;
use async_trait::async_trait;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{
    ArchiveBroker, ArchiveError, AsyncDequeueBroker, AsyncLeaseBroker, AsyncRedisExecutor, Broker,
    Clock, CompleteBroker, CompleteError, DequeueBroker, DequeueError, EnqueuePlan, ForwardBroker,
    LeaseBroker, RecoverBroker, RedisArg, RedisScript, RequeueBroker, RequeueError, RetryBroker,
    RetryError, Task, TaskMessage, TaskOption, TaskState,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecutorCall {
    Sadd {
        key: String,
        member: String,
    },
    ZaddExisting {
        key: String,
        score: i64,
        member: String,
    },
    EvalScriptInt {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
    EvalScriptBytes {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
    EvalScriptByteVec {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
    EvalScriptStatus {
        script: RedisScript,
        keys: Vec<String>,
        args: Vec<RedisArg>,
    },
}

#[derive(Debug)]
struct FakeExecutor {
    calls: Vec<ExecutorCall>,
    script_int_results: Vec<i64>,
    script_bytes_results: Vec<Option<Vec<u8>>>,
    script_byte_vec_results: Vec<Vec<Vec<u8>>>,
    script_status_results: Vec<String>,
    zadd_existing_results: Vec<usize>,
    sadd_error: Option<RedisExecutorError>,
    zadd_error: Option<RedisExecutorError>,
    script_error: Option<RedisExecutorError>,
}

impl Default for FakeExecutor {
    fn default() -> Self {
        Self {
            calls: Vec::new(),
            script_int_results: vec![1],
            script_bytes_results: Vec::new(),
            script_byte_vec_results: Vec::new(),
            script_status_results: vec!["OK".to_owned()],
            zadd_existing_results: vec![1],
            sadd_error: None,
            zadd_error: None,
            script_error: None,
        }
    }
}

impl RedisExecutor for FakeExecutor {
    fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        self.calls.push(ExecutorCall::Sadd {
            key: key.to_owned(),
            member: member.to_owned(),
        });
        if let Some(error) = self.sadd_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError> {
        self.calls.push(ExecutorCall::ZaddExisting {
            key: key.to_owned(),
            score,
            member: member.to_owned(),
        });
        if let Some(error) = self.zadd_error.clone() {
            return Err(error);
        }
        Ok(self.zadd_existing_results.remove(0))
    }

    fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptInt {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_int_results.remove(0))
    }

    fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptBytes {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_bytes_results.remove(0))
    }

    fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptByteVec {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_byte_vec_results.remove(0))
    }

    fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, RedisExecutorError> {
        self.calls.push(ExecutorCall::EvalScriptStatus {
            script: call.script(),
            keys: call.keys().to_vec(),
            args: call.args().to_vec(),
        });
        if let Some(error) = self.script_error.clone() {
            return Err(error);
        }
        Ok(self.script_status_results.remove(0))
    }
}

#[async_trait]
impl AsyncRedisExecutor for FakeExecutor {
    async fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        RedisExecutor::sadd(self, key, member)
    }

    async fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError> {
        RedisExecutor::zadd_existing(self, key, score, member)
    }

    async fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
        RedisExecutor::eval_script_int(self, call)
    }

    async fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        RedisExecutor::eval_script_bytes(self, call)
    }

    async fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        RedisExecutor::eval_script_byte_vec(self, call)
    }

    async fn eval_script_status(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<String, RedisExecutorError> {
        RedisExecutor::eval_script_status(self, call)
    }
}

#[test]
fn executes_publish_then_enqueue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [TaskOption::queue("critical")],
    );
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.enqueue(&plan).unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0],
        ExecutorCall::Sadd {
            key: "asynq:queues".to_owned(),
            member: "critical".to_owned()
        }
    );
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[1] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::Enqueue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert!(matches!(args[0], RedisArg::Bytes(_)));
    assert_eq!(args[1], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
}

#[tokio::test]
async fn async_broker_executes_publish_then_enqueue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [TaskOption::queue("critical")],
    );
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let mut broker = AsyncRedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.enqueue(&plan).await.unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0],
        ExecutorCall::Sadd {
            key: "asynq:queues".to_owned(),
            member: "critical".to_owned()
        }
    );
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[1] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::Enqueue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert!(matches!(args[0], RedisArg::Bytes(_)));
    assert_eq!(args[1], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
}

#[tokio::test]
async fn async_broker_dequeues_first_available_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_bytes_results: vec![None, Some(msg.encode_to_vec())],
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let task = broker
        .dequeue_with_now(&["empty".to_owned(), "critical".to_owned()], now)
        .await
        .unwrap();

    assert_eq!(task.message(), &msg);
    assert_eq!(
        task.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(broker.executor().calls.len(), 2);
    let ExecutorCall::EvalScriptBytes { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::Dequeue);
    assert_eq!(
        keys,
        &[
            "asynq:{empty}:pending".to_owned(),
            "asynq:{empty}:active".to_owned(),
            "asynq:{empty}:lease".to_owned(),
            "asynq:{empty}:t:".to_owned(),
            "asynq:{empty}:paused".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::I64(1_700_000_030)]);
}

#[tokio::test]
async fn async_broker_completes_retained_task_with_mark_as_complete_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = 300;
    let executor = FakeExecutor::default();
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    broker.complete_with_now(&msg, now).await.unwrap();

    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::MarkAsComplete);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:completed".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_300));
    assert!(matches!(args[3], RedisArg::Bytes(_)));
}

#[tokio::test]
async fn async_broker_retries_failed_task_with_retry_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor::default();
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    broker
        .retry_with_now(&msg, now, retry_at, "handler failed", true)
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected retry script call");
    };
    assert_eq!(*script, RedisScript::Retry);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:failed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_060));
    assert_eq!(args[4], RedisArg::String("1".to_owned()));
}

#[tokio::test]
async fn async_broker_archives_failed_task_with_archive_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor::default();
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    broker
        .archive_with_now(&msg, now, now, "max retry exhausted", true)
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected archive script call");
    };
    assert_eq!(*script, RedisScript::Archive);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:failed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000));
    assert_eq!(args[4], RedisArg::String("1".to_owned()));
}

#[tokio::test]
async fn async_broker_requeues_active_task_with_requeue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = AsyncRedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.requeue_with_now(&msg).await.unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected requeue script call");
    };
    assert_eq!(*script, RedisScript::Requeue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::String("task-id".to_owned())]);
}

#[tokio::test]
async fn async_broker_requeue_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let error = broker.requeue_with_now(&msg).await.unwrap_err();

    assert_eq!(error, RequeueError::NotFound);
}

#[tokio::test]
async fn async_broker_maps_task_id_conflict_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).await.unwrap_err();

    assert_eq!(error, BrokerError::TaskIdConflict);
}

#[tokio::test]
async fn async_broker_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        sadd_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).await.unwrap_err();

    assert_eq!(error, BrokerError::Other("connection closed".to_owned()));
}

#[tokio::test]
async fn async_broker_complete_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = 30;
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let error = broker.complete_with_now(&msg, now).await.unwrap_err();

    assert_eq!(error, CompleteError::Other("connection closed".to_owned()));
}

#[tokio::test]
async fn async_broker_forwards_scheduled_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![2],
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let moved = broker
        .forward_with_now("critical", now, true)
        .await
        .unwrap();

    assert_eq!(moved, 2);
    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::I64(1_700_000_000));
    assert_eq!(args[1], RedisArg::I64(1_700_000_000_000_000_000));
}

#[tokio::test]
async fn async_broker_forwards_retry_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![1],
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let moved = broker
        .forward_with_now("critical", now, false)
        .await
        .unwrap();

    assert_eq!(moved, 1);
    let ExecutorCall::EvalScriptInt { script, keys, .. } = &broker.executor().calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
}

#[tokio::test]
async fn async_broker_recovers_expired_leases_to_retry_or_archive() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut retry_msg = TaskMessage::from_task(&Task::new("email:welcome", b"retry".to_vec()));
    retry_msg.id = "retry-id".to_owned();
    retry_msg.queue = "critical".to_owned();
    retry_msg.retry = 3;
    retry_msg.retried = 1;
    let mut archive_msg = TaskMessage::from_task(&Task::new("email:welcome", b"archive".to_vec()));
    archive_msg.id = "archive-id".to_owned();
    archive_msg.queue = "critical".to_owned();
    archive_msg.retry = 1;
    archive_msg.retried = 1;
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![retry_msg.encode_to_vec(), archive_msg.encode_to_vec()]],
        script_status_results: vec!["OK".to_owned(), "OK".to_owned()],
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let result = broker
        .recover_expired_leases_with_now("critical", now, retry_at, "lease expired")
        .await
        .unwrap();

    assert_eq!(result.retried(), 1);
    assert_eq!(result.archived(), 1);
    assert_eq!(broker.executor().calls.len(), 3);
    let ExecutorCall::EvalScriptByteVec { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected list expired lease script call");
    };
    assert_eq!(*script, RedisScript::ListLeaseExpired);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::I64(1_700_000_000)]);
    let ExecutorCall::EvalScriptStatus { script, .. } = &broker.executor().calls[1] else {
        panic!("expected retry script call");
    };
    assert_eq!(*script, RedisScript::Retry);
    let ExecutorCall::EvalScriptStatus { script, .. } = &broker.executor().calls[2] else {
        panic!("expected archive script call");
    };
    assert_eq!(*script, RedisScript::Archive);
}

#[tokio::test]
async fn async_broker_extends_existing_lease() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = AsyncRedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let extension = broker
        .extend_lease_with_now("critical", "task-id", now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExisting {
            key: "asynq:{critical}:lease".to_owned(),
            score: 1_700_000_030,
            member: "task-id".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_reports_missing_lease_without_creating_one() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zadd_existing_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let extension = broker
        .extend_lease_with_now("critical", "task-id", now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
}

#[tokio::test]
async fn async_broker_trait_extends_existing_lease() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = AsyncRedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    AsyncLeaseBroker::extend_lease(&mut broker, "critical", "task-id")
        .await
        .unwrap();

    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExisting {
            key: "asynq:{critical}:lease".to_owned(),
            score: 1_700_000_030,
            member: "task-id".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_trait_dequeues_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_bytes_results: vec![Some(msg.encode_to_vec())],
        ..FakeExecutor::default()
    };
    let mut broker = AsyncRedisBroker::with_clock(executor, TestClock(now));

    let dequeued = AsyncDequeueBroker::dequeue(&mut broker, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(dequeued.message().id, "task-id");
}

#[test]
fn executes_unique_scheduled_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue("critical"),
            TaskOption::process_in(Duration::from_secs(60)),
            TaskOption::unique(Duration::from_secs(300)),
        ],
    );
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    assert_eq!(plan.state(), TaskState::Scheduled);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.enqueue(&plan).unwrap();

    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::ScheduleUnique);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[1], RedisArg::I64(360));
    assert_eq!(args[2], RedisArg::I64(1_700_000_060));
    assert!(matches!(args[3], RedisArg::Bytes(_)));
}

#[test]
fn maps_unique_duplicate_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [TaskOption::unique(Duration::from_secs(300))],
    );
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        script_int_results: vec![-1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).unwrap_err();

    assert_eq!(error, BrokerError::DuplicateTask);
}

#[test]
fn maps_task_id_conflict_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).unwrap_err();

    assert_eq!(error, BrokerError::TaskIdConflict);
}

#[test]
fn maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        sadd_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).unwrap_err();

    assert_eq!(error, BrokerError::Other("connection closed".to_owned()));
}

#[test]
fn dequeues_first_available_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_bytes_results: vec![None, Some(msg.encode_to_vec())],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let task = broker
        .dequeue(&["empty".to_owned(), "critical".to_owned()])
        .unwrap();

    assert_eq!(task.message(), &msg);
    assert_eq!(
        task.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(broker.executor().calls.len(), 2);
    let ExecutorCall::EvalScriptBytes { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected dequeue script call");
    };
    assert_eq!(*script, RedisScript::Dequeue);
    assert_eq!(
        keys,
        &[
            "asynq:{empty}:pending".to_owned(),
            "asynq:{empty}:active".to_owned(),
            "asynq:{empty}:lease".to_owned(),
            "asynq:{empty}:t:".to_owned(),
            "asynq:{empty}:paused".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::I64(1_700_000_030)]);
}

#[test]
fn dequeue_reports_no_processable_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_bytes_results: vec![None],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.dequeue(&["critical".to_owned()]).unwrap_err();

    assert_eq!(error, DequeueError::NoProcessableTask);
}

#[test]
fn dequeue_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.dequeue(&["critical".to_owned()]).unwrap_err();

    assert_eq!(error, DequeueError::Other("connection closed".to_owned()));
}

#[test]
fn completes_zero_retention_task_with_done_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.complete(&msg).unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected complete script call");
    };
    assert_eq!(*script, RedisScript::Done);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
}

#[test]
fn completes_retained_task_with_mark_as_complete_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = 300;
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.complete(&msg).unwrap();

    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected complete script call");
    };
    assert_eq!(*script, RedisScript::MarkAsComplete);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:completed".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(args[2], RedisArg::I64(1_700_000_300));
    assert!(matches!(args[3], RedisArg::Bytes(_)));
}

#[test]
fn complete_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.complete(&msg).unwrap_err();

    assert_eq!(error, CompleteError::NotFound);
}

#[test]
fn retries_failed_task_with_retry_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .retry(&msg, retry_at, "handler failed", true)
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected retry script call");
    };
    assert_eq!(*script, RedisScript::Retry);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:failed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_060));
    assert_eq!(args[4], RedisArg::String("1".to_owned()));
}

#[test]
fn retry_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .retry(&msg, now + Duration::from_secs(60), "handler failed", true)
        .unwrap_err();

    assert_eq!(error, RetryError::NotFound);
}

#[test]
fn archives_failed_task_with_archive_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .archive(&msg, now, "max retry exhausted", true)
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected archive script call");
    };
    assert_eq!(*script, RedisScript::Archive);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:failed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000));
    assert_eq!(args[4], RedisArg::String("1".to_owned()));
}

#[test]
fn archive_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .archive(&msg, now, "max retry exhausted", true)
        .unwrap_err();

    assert_eq!(error, ArchiveError::NotFound);
}

#[test]
fn requeues_active_task_with_requeue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.requeue(&msg).unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected requeue script call");
    };
    assert_eq!(*script, RedisScript::Requeue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::String("task-id".to_owned())]);
}

#[test]
fn requeue_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.requeue(&msg).unwrap_err();

    assert_eq!(error, RequeueError::NotFound);
}

#[test]
fn forwards_scheduled_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![2],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker.forward_scheduled("critical").unwrap();

    assert_eq!(moved, 2);
    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::I64(1_700_000_000));
    assert_eq!(args[1], RedisArg::I64(1_700_000_000_000_000_000));
}

#[test]
fn forwards_retry_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker.forward_retry("critical").unwrap();

    assert_eq!(moved, 1);
    let ExecutorCall::EvalScriptInt { script, keys, .. } = &broker.executor().calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
}

#[test]
fn recovers_expired_leases_to_retry_or_archive() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut retry_msg = TaskMessage::from_task(&Task::new("email:welcome", b"retry".to_vec()));
    retry_msg.id = "retry-id".to_owned();
    retry_msg.queue = "critical".to_owned();
    retry_msg.retry = 3;
    retry_msg.retried = 1;
    let mut archive_msg = TaskMessage::from_task(&Task::new("email:welcome", b"archive".to_vec()));
    archive_msg.id = "archive-id".to_owned();
    archive_msg.queue = "critical".to_owned();
    archive_msg.retry = 1;
    archive_msg.retried = 1;
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![retry_msg.encode_to_vec(), archive_msg.encode_to_vec()]],
        script_status_results: vec!["OK".to_owned(), "OK".to_owned()],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let result = broker
        .recover_expired_leases("critical", retry_at, "lease expired")
        .unwrap();

    assert_eq!(result.retried(), 1);
    assert_eq!(result.archived(), 1);
    assert_eq!(broker.executor().calls.len(), 3);
    let ExecutorCall::EvalScriptByteVec { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected list expired lease script call");
    };
    assert_eq!(*script, RedisScript::ListLeaseExpired);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::I64(1_700_000_000)]);
    let ExecutorCall::EvalScriptStatus { script, .. } = &broker.executor().calls[1] else {
        panic!("expected retry script call");
    };
    assert_eq!(*script, RedisScript::Retry);
    let ExecutorCall::EvalScriptStatus { script, .. } = &broker.executor().calls[2] else {
        panic!("expected archive script call");
    };
    assert_eq!(*script, RedisScript::Archive);
}

#[test]
fn extends_existing_lease() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let extension = broker.extend_lease("critical", "task-id").unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExisting {
            key: "asynq:{critical}:lease".to_owned(),
            score: 1_700_000_030,
            member: "task-id".to_owned(),
        }]
    );
}

#[test]
fn reports_missing_lease_without_creating_one() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zadd_existing_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let extension = broker.extend_lease("critical", "task-id").unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
}

#[derive(Debug, Clone, Copy)]
struct TestClock(SystemTime);

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}
