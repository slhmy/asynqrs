# Async Worker Dequeue, Complete, Retry, Archive, Forward, Recover, And Lease

这份文档记录当前 worker 侧保留的异步生命周期实现：`AsyncProcessor`
从 Redis pending queue 取出任务，移动到 active queue，写入 lease，调用 async
handler；处理成功后完成任务，处理失败后把任务移入 retry set，重试耗尽后归档到
archived set；到期 scheduled/retry 任务可以 forward 回 pending；lease 过期的
active 任务可以按 retry/archive 规则恢复；正在执行的 active 任务可以续约 lease。
`AsyncServer` 提供 Tokio-native worker 主循环，并通过 `tokio::sync::watch`
接收 shutdown signal。

实现参考 Asynq v0.26.0：

- `RDB.Dequeue`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>
- `RDB.Done` / `RDB.MarkAsComplete`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>
- `RDB.Retry`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>
- `RDB.Requeue`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506>
- `RDB.ForwardIfReady` / `forwardCmd`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900>
- recoverer lease-expired path：
  <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go>
- `processor.go` handler / lease extender flow：
  <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>
- `Server.Run` / `Server.Start`：
  <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>

## AsyncProcessor

`AsyncProcessor` 是当前 worker 执行器。它每次执行一个任务：

1. 调用 `AsyncDequeueBroker::dequeue`。
2. 把 `TaskMessage` 转成公开 `Task`，并调用 `AsyncHandler`。
3. handler 成功时调用 `AsyncCompleteBroker::complete`。
4. handler 返回 `HandlerError::Failed` 时，如果仍有 retry 次数，调用
   `AsyncRetryBroker::retry`；否则调用 `AsyncArchiveBroker::archive`。
5. handler 返回 `HandlerError::SkipRetry` 时直接 archive。
6. handler 返回 `HandlerError::RevokeTask` 时按 Asynq 的 done 路径删除任务。
7. shutdown 取消 in-flight 任务时，通过 `AsyncRequeueBroker::requeue` 放回 pending。

`DefaultRetryDelay` 按 Asynq v0.26.0 的指数退避公式计算下一次 retry 时间：
`retried^4 + 15 + random(0..30) * (retried + 1)` 秒。`DefaultIsFailure`
默认把所有 handler error 都视为 failure。

`AsyncProcessor` 会根据 `TaskMessage.timeout` 和 `TaskMessage.deadline` 计算执行截止
时间；两者同时存在时取更早的时间。handler 超时或 deadline 已过期时，会以
`context deadline exceeded` 作为 handler failure，继续走 retry/archive 路径。

`AsyncExtendLeaseWhileProcessing` 可以在 async handler 运行期间按固定间隔调用
`AsyncLeaseBroker::extend_lease`。handler 返回、失败、panic、timeout 或 lease
extension 失败时都会停止续约；lease extension 失败会作为 `ProcessorError::Lease`
返回并中断当前任务处理。

## AsyncServer

`AsyncServer` 持有一个 `AsyncWorkerProcessor`、一组队列和 idle sleep 配置，重复执行：

1. 等待 `tokio::sync::watch` shutdown signal。
2. 调用 `AsyncWorkerProcessor::run_maintenance`。
3. 对每个队列 forward 到期 scheduled/retry task，并恢复 lease-expired active task。
4. 调用 `AsyncWorkerProcessor::run_once`。
5. 任务完成、重试、归档或撤销时更新 `ServerRunSummary`。
6. 没有可处理任务时记录 idle poll，并通过 `AsyncSleeper` sleep。
7. shutdown 到来时取消正在等待的 `run_once`，调用 processor shutdown hook，然后返回 summary。

```rust,no_run
use asynq_rs::{
    AsyncProcessor, AsyncRedisBroker, AsyncRedisConnectionExecutor, AsyncServer, HandlerError, Task,
};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let redis_client = redis::Client::open("redis://127.0.0.1:6379")?;
let connection = redis_client.get_multiplexed_async_connection().await?;
let executor = AsyncRedisConnectionExecutor::new(connection);
let broker = AsyncRedisBroker::new(executor);
let processor = AsyncProcessor::new(broker, |task: &Task| {
    if task.type_name() == "email:welcome" {
        Ok(())
    } else {
        Err(HandlerError::failed("unsupported task type"))
    }
});

let mut server = AsyncServer::new(processor, ["critical", "default"])?;
let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
let summary = server.run_until_stopped(shutdown_rx).await?;
println!("processed={}, idle_polls={}", summary.processed(), summary.idle_polls());
# Ok(())
# }
```

## Redis broker

生产 Redis worker 路径只保留异步边界：

- `AsyncRedisExecutor`
- `AsyncRedisConnectionExecutor`
- `AsyncRedisBroker`

`AsyncRedisBroker` 提供 enqueue、dequeue、complete、retry、archive、requeue、forward、
recover 和 extend-lease 路径，复用纯 plan 类型和 Asynq v0.26.0 的 Redis script 语义。

## 当前差距

- forwarder/recoverer 仍在 `run_maintenance` 中执行固定维护 pass，还没有独立的上游式定时器。
- async server lifecycle 配置还没有覆盖上游所有选项。
