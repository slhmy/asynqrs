# Worker Dequeue, Complete, And Retry

这份文档记录当前 worker 侧已经实现的最小生命周期路径：从 Redis pending
queue 取出一个任务，移动到 active queue，写入 lease；处理成功后完成任务，
处理失败后把任务移入 retry set。

实现参考 Asynq v0.26.0：

- `RDB.Dequeue`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>
- `RDB.Done` / `RDB.MarkAsComplete`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>
- `RDB.Retry`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>
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

## Redis 测试

`tests/redis_enqueue.rs` 现在覆盖了 pending enqueue 后的 dequeue、complete 和
retry 状态迁移：

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

本地运行：

```sh
cargo test --test redis_enqueue
```

没有 Docker 且没有设置 `ASYNQ_RS_REDIS_URL` 时，Redis 写入用例会跳过。
CI 会通过 Redis service 设置 `ASYNQ_RS_REDIS_URL`，因此会连接真实 Redis。

## 还没实现的部分

- worker `Server` / `Processor` 主循环。
- archive：超过重试次数后归档。
- lease 续约和 lease 过期恢复。
- scheduled / retry task 的调度迁移。
- completed task 过期清理。
