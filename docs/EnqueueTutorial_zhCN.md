# Enqueue Tutorial

这份文档按当前代码解释一次任务从用户 API 到 Redis 写入计划的完整路径。

当前项目还没有真实 Redis client。现在已经实现的是三层纯模型：

1. `Task` / `TaskOption`：用户想提交什么任务。
2. `EnqueuePlan`：这个任务应该以什么元数据和状态入队。
3. `RedisEnqueuePlan`：如果要写 Redis，应调用哪些 Asynq enqueue 脚本。

这些实现参考 Asynq v0.26.0：

- `Task` / `TaskOption`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L22-L73>
  和 <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L47-L163>
- `Client.EnqueueContext`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L331>
- Redis enqueue scripts：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>

## 1. 创建任务

`Task` 是用户侧的任务描述，包含任务类型、payload、headers 和 options。

```rust
use asynq_rs::Task;

let mut task = Task::new("email:welcome", br#"{"user_id":42}"#.to_vec());
task.insert_header("trace-id", "abc");
```

`Task::with_headers` 可以一次性传入 headers：

```rust
use asynq_rs::Task;

let task = Task::with_headers(
    "image:resize",
    b"payload".to_vec(),
    [("trace-id", "abc"), ("tenant", "acme")],
);
```

## 2. 添加入队选项

`TaskOption` 对应 Asynq 的 enqueue options。当前支持：

- `max_retry`
- `queue`
- `task_id`
- `timeout`
- `deadline`
- `unique`
- `process_at`
- `process_in`
- `retention`
- `group`

示例：

```rust
use std::time::Duration;
use asynq_rs::{Task, TaskOption};

let task = Task::new_with_options(
    "email:welcome",
    b"{}".to_vec(),
    [
        TaskOption::queue("critical"),
        TaskOption::max_retry(3),
        TaskOption::timeout(Duration::from_secs(30)),
        TaskOption::unique(Duration::from_secs(300)),
    ],
);
```

同一个字段被多次设置时，后面的 option 覆盖前面的 option。`Client` 的
`enqueue_with_options` 会在 task 自带 options 之后继续应用调用侧 options，
所以调用侧 options 优先级更高。

## 3. 生成 EnqueuePlan

`EnqueuePlan` 是当前入队语义的核心。它把 `Task` 和 options 合成为内部
`TaskMessage`，并决定任务初始状态。

```rust
use std::time::{Duration, UNIX_EPOCH};
use asynq_rs::{EnqueuePlan, Task, TaskOption, TaskState};

let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
let task = Task::new_with_options(
    "email:welcome",
    b"payload".to_vec(),
    [TaskOption::queue("critical")],
);

let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();

assert_eq!(plan.state(), TaskState::Pending);
assert_eq!(plan.message().id, "task-id");
assert_eq!(plan.message().queue, "critical");
```

默认值和 upstream 对齐：

- 默认 queue：`default`
- 默认 retry：`25`
- 默认 timeout：`30` 分钟
- 如果设置了 `deadline`，默认 timeout 不会再自动填充

状态选择规则：

- 没有未来执行时间，也没有 group：`Pending`
- `ProcessAt` / `ProcessIn` 指向未来：`Scheduled`
- 没有未来执行时间，但设置了 `Group`：`Aggregating`

`Unique` 会根据 queue、task type 和 payload 生成 Asynq 兼容的 unique key：

```text
asynq:{queue}:unique:{task_type}:{payload_md5}
```

定时唯一任务的 lock TTL 会包含等待时间，也就是：

```text
unique_lock_ttl = process_delay + unique_ttl
```

## 4. 通过 Client 调用入队 API

`Client` 是公开的 enqueue API。它负责：

1. 获取当前时间。
2. 生成默认 task id。
3. 构造 `EnqueuePlan`。
4. 把 plan 交给 `Broker`。
5. 返回 `EnqueueResult`。

当前已经有 `RedisBroker` 骨架，但还没有接真实 Redis crate。普通测试或学习时仍
可以自己实现 `Broker` trait。

```rust
use asynq_rs::{Broker, BrokerError, Client, EnqueuePlan, Task, TaskOption};

#[derive(Default)]
struct RecordingBroker {
    plans: Vec<EnqueuePlan>,
}

impl Broker for RecordingBroker {
    fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.plans.push(plan.clone());
        Ok(())
    }
}

let mut client = Client::new(RecordingBroker::default());
let task = Task::new_with_options(
    "email:welcome",
    b"{}".to_vec(),
    [TaskOption::queue("critical")],
);

let result = client.enqueue(&task).unwrap();

assert_eq!(result.queue(), "critical");
```

测试或特殊调用可以注入 task id 生成器和 clock。当前代码通过 `Client::with_parts`
支持这个能力。

## 5. 生成 RedisEnqueuePlan

`RedisEnqueuePlan` 不执行 Redis 命令。它只表达“真实 Redis broker 应该做什么”。

每个计划包含：

1. `PublishQueue`：把队列名加入 `asynq:queues`。
2. `RunScript`：调用一个 upstream enqueue Lua 脚本。

示例：

```rust
use std::time::{Duration, UNIX_EPOCH};
use asynq_rs::{EnqueuePlan, RedisEnqueuePlan, RedisEnqueueOperation, Task, TaskOption};

let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
let task = Task::new_with_options(
    "email:welcome",
    b"payload".to_vec(),
    [TaskOption::queue("critical")],
);
let enqueue_plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

assert!(matches!(
    redis_plan.operations()[0],
    RedisEnqueueOperation::PublishQueue { .. }
));
```

状态和脚本的映射：

| `EnqueuePlan` 状态 | 无 `Unique` | 有 `Unique` |
| --- | --- | --- |
| `Pending` | `Enqueue` | `EnqueueUnique` |
| `Scheduled` | `Schedule` | `ScheduleUnique` |
| `Aggregating` | `AddToGroup` | `AddToGroupUnique` |

`TaskMessage` 会通过 protobuf 编码成 bytes，作为脚本参数传入。这是 Redis 中
task body 的内容。

## 6. 使用 RedisBroker 骨架

`RedisBroker` 实现了 `Broker`。它接收 `EnqueuePlan`，构造 `RedisEnqueuePlan`，
然后通过 `RedisExecutor` 执行：

- `sadd(key, member)`：发布队列名。
- `run_enqueue_script(script, keys, args)`：执行对应 enqueue 脚本。

`RedisExecutor` 是当前真实 Redis client 的适配边界。代码里已经提供了同步
`redis` crate 的适配器：`RedisConnectionExecutor<C>`。`C` 可以是实现了
redis-rs `ConnectionLike` 的连接类型。

最小组合大致是：

```rust,no_run
use asynq_rs::{Client, RedisBroker, RedisConnectionExecutor};

let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
let connection = redis_client.get_connection().unwrap();
let executor = RedisConnectionExecutor::new(connection);
let broker = RedisBroker::new(executor);
let mut client = Client::new(broker);
```

每个 `RedisEnqueueScript` 都可以查询脚本元数据：

- `name()`：脚本名，例如 `enqueue_unique`。
- `source()`：固定到 Asynq v0.26.0 的 Lua 源码。
- `key_count()` / `arg_count()`：调用形状。
- `result_for_code(code)`：返回码语义。

`RedisBroker` 会在调用 executor 前校验 `RedisScriptCall` 的 key/arg 数量。

脚本返回值会映射成 `BrokerError`：

| 脚本结果 | 含义 |
| --- | --- |
| `1` | 成功 |
| `0` | `TaskIdConflict` |
| `-1` 且为 unique 脚本 | `DuplicateTask` |

## 7. 当前还没实现的部分

当前代码还没有：

- 异步 Redis executor。
- Redis 连接池封装。
- 真实 Redis 集成测试。
- worker 侧取任务、执行、ack、retry、archive、complete。
- `ResultWriter` 等 worker 执行期能力。

下一步比较自然的是补真实 Redis 集成测试或连接池适配：

1. 用本地 Redis 或 testcontainers 启动测试 Redis。
2. 通过 `Client<RedisBroker<RedisConnectionExecutor<_>>>` 入队任务。
3. 读取 Redis key，验证 task body、pending/scheduled/group/unique 状态。
4. 再决定是否引入 async runtime 和连接池。
