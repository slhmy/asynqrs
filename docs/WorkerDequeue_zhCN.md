# Worker Dequeue, Complete, Retry, Archive, Forward, And Recover

这份文档记录当前 worker 侧已经实现的最小生命周期路径：从 Redis pending
queue 取出一个任务，移动到 active queue，写入 lease；处理成功后完成任务，
处理失败后把任务移入 retry set，重试耗尽后归档到 archived set；到期的
scheduled/retry 任务可以被 forward 回 pending；lease 过期的 active 任务
可以按 retry/archive 规则恢复。

实现参考 Asynq v0.26.0：

- `RDB.Dequeue`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>
- `RDB.Done` / `RDB.MarkAsComplete`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>
- `RDB.Retry`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>
- archive 相关 task state 和 Redis lifecycle：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>
- `RDB.ForwardIfReady` / `forwardCmd`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900>
- recoverer lease-expired path / `RDB.ListLeaseExpired`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go>
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>
- `LeaseDuration`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L46-L52>
- `statsTTL`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L54-L60>
- broker 接口：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>

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
clock-skew cutoff、默认 retry delay 计算和 lease extension 还没有建模。

## Redis 测试

`tests/redis_enqueue.rs` 现在覆盖了 pending enqueue 后的 dequeue、complete、
retry、archive、forward 和 recover 状态迁移：

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
- forward scheduled/retry 会把到期任务移回 pending
- 未到期 scheduled/retry 任务不会被移动
- recover 会把 lease-expired active task 送入 retry 或 archived
- recover 会清理 active/lease，更新失败字段和 processed/failed counters

本地运行：

```sh
cargo test --test redis_enqueue
```

没有 Docker 且没有设置 `ASYNQ_RS_REDIS_URL` 时，Redis 写入用例会跳过。
CI 会通过 Redis service 设置 `ASYNQ_RS_REDIS_URL`，因此会连接真实 Redis。

## 还没实现的部分

- worker `Server` / `Processor` 主循环。
- lease 续约。
- server-side recoverer 定时循环。
- server-side forwarder 循环。
- completed task 过期清理。
