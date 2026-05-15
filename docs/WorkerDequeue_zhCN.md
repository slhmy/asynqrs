# Worker Dequeue, Complete, Retry, Archive, Forward, Recover, And Lease

这份文档记录当前 worker 侧已经实现的最小生命周期路径：`Processor`
可以从 Redis pending queue 取出一个任务，移动到 active queue，写入
lease，调用用户 handler；处理成功后完成任务，处理失败后把任务移入 retry
set，重试耗尽后归档到 archived set；到期的 scheduled/retry 任务可以被
forward 回 pending；lease 过期的 active 任务可以按 retry/archive 规则恢复；
正在执行的 active 任务可以续约 lease。`Server` 提供了第一个同步 worker
主循环，可以持续调用 `Processor::run_once`，在空队列时 sleep，并由调用方
提供 shutdown signal 停止拉取新任务。

实现参考 Asynq v0.26.0：

- `RDB.Dequeue`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>
- `RDB.Done` / `RDB.MarkAsComplete`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>
- `RDB.Retry`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>
- archive 相关 task state 和 Redis lifecycle：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>
- `RDB.Requeue`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506>
- `RDB.ForwardIfReady` / `forwardCmd`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900>
- recoverer lease-expired path / `RDB.ListLeaseExpired`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go>
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>
- `RDB.ExtendLease`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>
- `LeaseDuration`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L46-L52>
- `statsTTL`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L54-L60>
- broker 接口：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>
- `Handler` / `HandlerFunc`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L622-L650>
- `ErrorHandler`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L277-L287>
- `RetryDelayFunc` / `IsFailure`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L291-L297>
  <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130>
- processor 成功/失败路由：
  <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>
- `Server.Run` / `Server.Start`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>

## Processor

`Processor` 是当前最小 worker 执行器。它每次执行一个任务：

1. 调用 `DequeueBroker::dequeue`。
2. 把 `TaskMessage` 转成公开 `Task`，并调用 handler。
3. handler 成功时调用 `CompleteBroker::complete`。
4. handler 返回 `HandlerError::Failed` 时，如果仍有 retry 次数，调用
   `RetryBroker::retry`；否则调用 `ArchiveBroker::archive`。
5. handler 返回 `HandlerError::SkipRetry` 时直接 archive。
6. handler 返回 `HandlerError::RevokeTask` 时按 Asynq 的 done 路径删除任务，
   不 retry，也不 archive。

```rust,no_run
use std::time::Duration;

use asynq_rs::{
    HandlerError, Processor, RedisBroker, RedisClientExecutor, Task,
};

let redis_client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
let executor = RedisClientExecutor::new(redis_client);
let broker = RedisBroker::new(executor);

let mut processor = Processor::with_retry_delay(
    broker,
    |task: &Task| {
        if task.type_name() == "email:welcome" {
            Ok(())
        } else {
            Err(HandlerError::failed("unsupported task type"))
        }
    },
    |_retried, _error, _task| Duration::from_secs(60),
);

let result = processor
    .run_once(&["critical".to_owned(), "default".to_owned()])
    .unwrap();

println!("processor result: {result:?}");
```

可以用 `with_is_failure` 决定 retry 路径中的 handler error 是否计入 failed
counters，用 `with_error_handler` 记录或上报 handler error：

```rust,no_run
use asynq_rs::{HandlerError, Processor, Task};

let processor = Processor::new(broker, |_task: &Task| {
    Err(HandlerError::failed("temporary remote outage"))
})
.with_is_failure(|error: &HandlerError| error.to_string() != "temporary remote outage")
.with_error_handler(|task: &Task, error: &HandlerError| {
    eprintln!("task type={} failed: {error}", task.type_name());
});
```

默认 `DefaultRetryDelay` 按 Asynq v0.26.0 的指数退避公式计算下一次 retry
时间：`retried^4 + 15 + random(0..30) * (retried + 1)` 秒。
默认 `DefaultIsFailure` 会把所有 handler error 都视为 failure；
`NoopErrorHandler` 不做任何处理。和上游一致，重试耗尽或 `SkipRetry` 后的
archive 路径仍按失败归档统计。

当前 `Processor` 仍是同步、单线程、一次处理一个任务。它还没有实现上游
server 的并发 worker 池、context timeout/deadline、后台 lease extender、
shutdown requeue 和 sync retry。

## Server

`Server` 是当前最小 worker 主循环。它持有一个 `Processor`、一组队列和
idle sleep 配置，重复执行：

1. 检查调用方提供的 `ShutdownSignal`。
2. 调用 `WorkerProcessor::run_once`。
3. 任务完成、重试、归档或撤销时更新 `ServerRunSummary`。
4. 没有可处理任务时记录 idle poll，并通过 `Sleeper` sleep。
5. shutdown signal 变为 true 后停止拉取新任务并返回 summary。

```rust,no_run
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use asynq_rs::{
    HandlerError, Processor, RedisBroker, RedisClientExecutor, Server, Task,
};

let redis_client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
let executor = RedisClientExecutor::new(redis_client);
let broker = RedisBroker::new(executor);
let processor = Processor::new(broker, |task: &Task| {
    if task.type_name() == "email:welcome" {
        Ok(())
    } else {
        Err(HandlerError::failed("unsupported task type"))
    }
});
let mut server = Server::new(processor, ["critical", "default"])
    .unwrap()
    .with_idle_sleep(Duration::from_secs(1));

let stopped = Arc::new(AtomicBool::new(false));
let mut shutdown = {
    let stopped = Arc::clone(&stopped);
    move || stopped.load(Ordering::Relaxed)
};

let summary = server.run_until_stopped(&mut shutdown).unwrap();
println!(
    "processed={}, idle_polls={}",
    summary.processed(),
    summary.idle_polls()
);
```

`SystemSleeper` 默认使用 `std::thread::sleep`，测试或嵌入式调用可以通过
`Server::with_sleeper` 注入自定义 sleeper。当前 shutdown 是 graceful
边界：signal 置位后停止拉取新任务；已经进入 handler 的任务会自然执行完。
真正的 in-flight cancellation、lease extender 和 shutdown requeue 还没有建模。

## Dequeue

`DequeueBroker` 是 worker 侧的最小 broker trait：

```rust,no_run
use asynq_rs::{CompleteBroker, DequeueBroker, RedisBroker, RedisClientExecutor};

let redis_client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
let executor = RedisClientExecutor::new(redis_client);
let mut broker = RedisBroker::new(executor);

let task = broker.dequeue(&["critical".to_owned(), "default".to_owned()]).unwrap();

println!("task id: {}", task.message().id);
println!("lease expires at: {:?}", task.lease_expires_at());

broker.complete(task.message()).unwrap();
```

`RedisBroker::dequeue` 会按传入队列顺序尝试取任务。每个队列执行一次
dequeue Lua 脚本：

1. 如果队列 paused，返回空。
2. 从 `pending` 右侧弹出任务 id，并推入 `active`。
3. 把 task hash 的 `state` 改为 `active`。
4. 删除 task hash 的 `pending_since`。
5. 把任务 id 写入 `lease` sorted set，score 是当前时间 + 30 秒。
6. 返回 task hash 中的 protobuf `msg` bytes。

当前默认 lease 时长是 30 秒，对应公开常量 `DEFAULT_LEASE_DURATION`。

如果所有队列都没有可处理任务，返回 `DequeueError::NoProcessableTask`。

## Complete

`CompleteBroker` 是 worker 处理成功后的最小 broker trait：

```rust,no_run
use asynq_rs::{CompleteBroker, DequeuedTask};

fn finish_task<B: CompleteBroker>(broker: &mut B, task: &DequeuedTask) {
    broker.complete(task.message()).unwrap();
}
```

`RedisBroker::complete` 会根据 task message 的 `retention` 自动选择脚本：

- `retention == 0`：执行 `Done` / `DoneUnique`，从 `active` list 和
  `lease` sorted set 中移除 task id，删除 task hash。
- `retention > 0`：执行 `MarkAsComplete` / `MarkAsCompleteUnique`，从
  `active` 和 `lease` 移除 task id，把 task hash state 写成
  `completed`，更新 message 的 `completed_at`，并把 task id 写入
  `completed` sorted set。score 是当前时间 + retention。
- unique task 完成时会释放仍由该 task 持有的 unique lock。
- 两条路径都会更新 daily processed counter 和 processed total counter。

如果 Redis 脚本报告 task 不存在，返回 `CompleteError::NotFound`。

## Retry

`RetryBroker` 是 worker 处理失败后的最小 broker trait：

```rust,no_run
use std::time::{Duration, SystemTime};

use asynq_rs::{DequeuedTask, RetryBroker};

fn retry_task<B: RetryBroker>(broker: &mut B, task: &DequeuedTask) {
    let retry_at = SystemTime::now() + Duration::from_secs(60);
    broker
        .retry(task.message(), retry_at, "handler failed", true)
        .unwrap();
}
```

`RedisBroker::retry` 执行 Asynq v0.26.0 的 retry Lua 脚本：

- 从 `active` list 和 `lease` sorted set 中移除 task id。
- 把 task id 写入 `retry` sorted set，score 是传入的 `retry_at`。
- 把 task hash state 写成 `retry`。
- 更新 protobuf message 的 `retried`、`error_msg`、`last_failed_at`。
- `is_failure == true` 时，更新 daily processed/failed counters 和对应 total counters。

如果 Redis 脚本报告 task 不存在，返回 `RetryError::NotFound`。

## Archive

`ArchiveBroker` 是 worker 放弃继续重试后的最小 broker trait：

```rust,no_run
use std::time::SystemTime;

use asynq_rs::{ArchiveBroker, DequeuedTask};

fn archive_task<B: ArchiveBroker>(broker: &mut B, task: &DequeuedTask) {
    broker
        .archive(
            task.message(),
            SystemTime::now(),
            "max retry exhausted",
            true,
        )
        .unwrap();
}
```

`RedisBroker::archive` 执行 archive 生命周期脚本：

- 从 `active` list 和 `lease` sorted set 中移除 task id。
- 把 task id 写入 `archived` sorted set，score 是传入的 `archived_at`。
- 把 task hash state 写成 `archived`。
- 更新 protobuf message 的 `retried`、`error_msg`、`last_failed_at`。
- `is_failure == true` 时，更新 daily processed/failed counters 和对应 total counters。

如果 Redis 脚本报告 task 不存在，返回 `ArchiveError::NotFound`。

## Requeue

`RequeueBroker` 是把 active task 放回 pending 的最小 broker trait：

```rust,no_run
use asynq_rs::{DequeuedTask, RequeueBroker};

fn put_back<B: RequeueBroker>(broker: &mut B, task: &DequeuedTask) {
    broker.requeue(task.message()).unwrap();
}
```

`RedisBroker::requeue` 执行 Asynq v0.26.0 的 requeue Lua 脚本：

- 从 `active` list 移除 task id。
- 从 `lease` sorted set 移除 task id。
- 把 task id `RPUSH` 回 `pending` list。
- 把 task hash state 写成 `pending`。
- 不更新 processed/failed counters。

如果 Redis 脚本报告 task 不存在，返回 `RequeueError::NotFound`。

## Forward

`ForwardBroker` 是把到期 scheduled/retry 任务重新放回 pending 的最小
broker trait：

```rust,no_run
use asynq_rs::ForwardBroker;

fn forward_due<B: ForwardBroker>(broker: &mut B) {
    let scheduled = broker.forward_scheduled("default").unwrap();
    let retry = broker.forward_retry("default").unwrap();

    println!("moved scheduled={scheduled}, retry={retry}");
}
```

`RedisBroker::forward_scheduled` 和 `RedisBroker::forward_retry` 都执行 Asynq
v0.26.0 的 forward 脚本：

- 从 `scheduled` 或 `retry` sorted set 找出到期 task id。
- 普通任务写回 `pending` list。
- task hash state 写成 `pending`，并写入 `pending_since`。
- 已移动的 task id 从源 sorted set 删除。
- 当前接口每次执行上游一批，最多移动 100 个 task id。

## Recover

`RecoverBroker` 是 worker 崩溃或 lease 过期后的最小恢复接口：

```rust,no_run
use std::time::{Duration, SystemTime};

use asynq_rs::RecoverBroker;

fn recover_expired<B: RecoverBroker>(broker: &mut B) {
    let retry_at = SystemTime::now() + Duration::from_secs(60);
    let result = broker
        .recover_expired_leases("default", retry_at, "asynq: task lease expired")
        .unwrap();

    println!(
        "recovered total={}, retried={}, archived={}",
        result.total(),
        result.retried(),
        result.archived()
    );
}
```

`RedisBroker::recover_expired_leases` 先执行 Asynq v0.26.0 的 lease-expired
listing 脚本，找出 `lease` sorted set 中已过期的 active task message。
随后按 recoverer 规则处理：

- `retried < retry`：调用 retry 路径，任务从 `active` / `lease` 移到
  `retry` sorted set。
- `retried >= retry`：调用 archive 路径，任务从 `active` / `lease` 移到
  `archived` sorted set。
- message 会更新 `retried`、`error_msg`、`last_failed_at`。
- 当前恢复把 lease-expired 视作 failure，因此会更新 processed/failed counters。

当前接口只恢复单个队列的一次扫描；server-side recoverer 定时循环、30 秒
clock-skew cutoff 和默认 retry delay 计算还没有建模。

## Lease Extension

`LeaseBroker` 是 worker 处理长任务时续约 active task lease 的最小接口：

```rust,no_run
use asynq_rs::{DequeuedTask, LeaseBroker};

fn keep_alive<B: LeaseBroker>(broker: &mut B, task: &DequeuedTask) {
    let extension = broker
        .extend_lease(&task.message().queue, &task.message().id)
        .unwrap();

    println!("lease expires at: {:?}", extension.expires_at());
}
```

`RedisBroker::extend_lease` 按 Asynq v0.26.0 的 `RDB.ExtendLease` 语义执行
`ZADD XX`：

- 只更新 `lease` sorted set 中已经存在的 task id。
- score 写成当前时间 + `DEFAULT_LEASE_DURATION`。
- 返回新的 lease 过期时间。
- task id 不存在时 Redis 不会创建新的 lease entry。

注意：Redis `ZADD XX` 更新已有成员时返回值也可能是 `0`，所以当前公开
接口只返回本次计算出的过期时间，不把返回值解释成“这个 task 是否存在”。

## Redis 测试

`tests/redis_enqueue.rs` 现在覆盖了 pending enqueue 后的 dequeue、complete、
retry、archive、requeue、forward、recover、lease extension 和 server loop
状态迁移：

- task hash state 从 `pending` 变成 `active`
- `pending_since` 被删除
- pending list 变空
- active list 包含 task id
- lease sorted set 包含 task id
- zero-retention task 完成后 task hash 被删除
- retained task 完成后进入 `completed` sorted set
- complete 会清理 active/lease 并更新 processed counters
- unique task complete 会释放 unique lock
- retry 会清理 active/lease，把 task id 写入 retry sorted set
- retry 会更新 message 的失败字段，并更新 processed/failed counters
- archive 会清理 active/lease，把 task id 写入 archived sorted set
- archive 会更新 message 的失败字段，并更新 processed/failed counters
- requeue 会清理 active/lease，把 task id 放回 pending 且不更新统计计数
- forward scheduled/retry 会把到期任务移回 pending
- 未到期 scheduled/retry 任务不会被移动
- recover 会把 lease-expired active task 送入 retry 或 archived
- recover 会清理 active/lease，更新失败字段和 processed/failed counters
- extend lease 会更新已有 active lease 的 score
- complete 后再次 extend lease 不会创建新的 lease entry
- server loop 会持续调用 processor，处理成功任务、失败 retry 任务，并在
  idle poll 后 sleep

本地运行：

```sh
cargo test --test redis_enqueue
```

没有 Docker 且没有设置 `ASYNQ_RS_REDIS_URL` 时，Redis 写入用例会跳过。
CI 会通过 Redis service 设置 `ASYNQ_RS_REDIS_URL`，因此会连接真实 Redis。

## 还没实现的部分

- worker 并发池。
- task context timeout/deadline。
- worker-side lease extender 定时循环。
- shutdown requeue。
- server-side recoverer 定时循环。
- server-side forwarder 循环。
- completed task 过期清理。
