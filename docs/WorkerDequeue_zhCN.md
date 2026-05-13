# Worker Dequeue

这份文档记录当前 worker 侧已经实现的最小能力：从 Redis pending queue
取出一个任务，移动到 active queue，并写入 lease。

实现参考 Asynq v0.26.0：

- `RDB.Dequeue`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>
- `LeaseDuration`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L46-L52>
- broker 接口：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>

## 当前行为

`DequeueBroker` 是 worker 侧的最小 broker trait：

```rust,no_run
use asynq_rs::{DequeueBroker, RedisBroker, RedisClientExecutor};

let redis_client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
let executor = RedisClientExecutor::new(redis_client);
let mut broker = RedisBroker::new(executor);

let task = broker.dequeue(&["critical".to_owned(), "default".to_owned()]).unwrap();

println!("task id: {}", task.message().id);
println!("lease expires at: {:?}", task.lease_expires_at());
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

## Redis 测试

`tests/redis_enqueue.rs` 现在覆盖了 pending enqueue 后的 dequeue 状态迁移：

- task hash state 从 `pending` 变成 `active`
- `pending_since` 被删除
- pending list 变空
- active list 包含 task id
- lease sorted set 包含 task id

本地运行：

```sh
cargo test --test redis_enqueue
```

没有 Docker 且没有设置 `ASYNQ_RS_REDIS_URL` 时，Redis 写入用例会跳过。
CI 会通过 Redis service 设置 `ASYNQ_RS_REDIS_URL`，因此会连接真实 Redis。

## 还没实现的部分

- worker `Server` / `Processor` 主循环。
- ack / done：任务成功后从 active 和 lease 中移除。
- retry：任务失败后按 retry policy 进入 retry set。
- archive：超过重试次数后归档。
- lease 续约和 lease 过期恢复。
- scheduled / retry task 的调度迁移。
