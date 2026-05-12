# Task And TaskMessage

Task 是用户侧看到的“任务”：

```rust
Task {
    type_name,
    payload,
    headers,
    opts,
}
```

它表达的是：我要执行一个什么类型的任务，以及执行它需要什么数据。
`opts` 存储的是用户传入的 `TaskOption`，例如队列名、任务 ID、
重试次数、超时、定时执行、唯一任务和分组。

TaskMessage 是内部侧用来入队、调度、重试、归档的“任务消息”：

```proto
message TaskMessage {
    string type = 1;
    bytes payload = 2;
    map<string, string> headers = 15;
    string id = 3;
    string queue = 4;
    int32 retry = 5;
    int32 retried = 6;
    string error_msg = 7;
    int64 timeout = 8;
    int64 deadline = 9;
    string unique_key = 10;
    int64 last_failed_at = 11;
    int64 retention = 12;
    int64 completed_at = 13;
    string group_key = 14;
}
```

也就是说它包含两类信息：

1. 任务本身
   - type
   - payload
   - headers
2. 队列系统运行需要的元数据
   - id
   - queue
   - retry
   - retried
   - error_msg
   - last_failed_at
   - timeout
   - deadline
   - unique_key
   - group_key
   - retention
   - completed_at

Asynq 会把这个 protobuf encode 成 bytes，存到 Redis 的 task key 里。然后 Redis 里的不同集合/list/zset 负责表达状态，例如 pending、active、scheduled、retry、archived、
completed。TaskMessage 本身不直接存 state，状态主要由它所在的 Redis key/集合决定。

所以一句话：

TaskMessage = Redis 中 task body 的 protobuf 编码内容，包含 task 数据和队列调度元数据；task 当前状态由 Redis key/集合位置表达。

当前代码里，裸 `TaskMessage::from_task` 只负责基础消息构造。真正的入队
默认值、状态选择、唯一任务 key、定时和分组语义由 `EnqueuePlan` 建模。
完整入队流程见 [EnqueueTutorial_zhCN.md](./EnqueueTutorial_zhCN.md)。
