# Redis Smoke Matrix

This matrix maps release-readiness lifecycle scenarios to current Redis
integration coverage. These tests live under `src/broker/redis/tests`.

Run the Redis smoke set serially in strict mode so one Redis instance or Docker
daemon failure fails the release gate instead of being mistaken for coverage:

```sh
ASYNQ_RS_REDIS_STRICT=1 cargo test broker::redis::tests:: -- --nocapture --test-threads=1
```

The project release gate script runs this command as part of the full checklist:

```sh
scripts/release-gate.sh
```

Before running the strict Redis smoke command, the release gate runs:

```sh
scripts/redis-smoke-preflight.sh
```

The preflight fails early unless `ASYNQ_RS_REDIS_URL` is set or Docker daemon
access is available for `testcontainers`. It is diagnostic only; passing the
preflight does not prove Redis reachability and does not replace the strict
Redis smoke command.
When it fails, it reports the missing Redis URL, local Redis CLI, and Docker
daemon evidence, then prints the release-gate commands to rerun after fixing the
environment.

The final release gate runs the same checklist twice:

```sh
scripts/final-release-gate.sh
```

The tests use `ASYNQ_RS_REDIS_URL` when set. Otherwise they try to start a Redis
container through `testcontainers`. Without `ASYNQ_RS_REDIS_STRICT=1`, a missing
Redis instance still skips Redis-dependent tests for normal local development.

GitHub CI runs this strict command against a Redis service container through
`ASYNQ_RS_REDIS_URL=redis://127.0.0.1:6379`. It intentionally does not rely on
Docker-in-Docker or `testcontainers` in CI. Local release verification should
use the same command with either `ASYNQ_RS_REDIS_URL` set to the Redis instance
that strict smoke should verify or a working Docker daemon for `testcontainers`.

| Scenario | Evidence |
| --- | --- |
| enqueue -> process -> complete | `server::async_server_with_redis_runtime_completes_task_and_stops` and `lifecycle::async_pending_enqueue_dequeue_and_complete_uses_redis_layout` |
| handler failure -> retry -> archive | `lifecycle::async_retry_records_failure_and_moves_task_to_retry_set` and `lifecycle::async_archive_trims_old_archived_tasks_and_records_failure_stats` |
| graceful shutdown while a task is active | `server::async_server_shutdown_requeues_in_flight_task` |
| cancellation pub/sub | `metadata::async_redis_cancel_pubsub_cancels_active_task` |
| scheduler enqueue | `scheduler::async_scheduler_run_once_enqueues_due_task_to_redis` |
| aggregation flush | `aggregation::async_aggregation_primitives_round_trip_group_tasks` |
| inspector reads runtime metadata | `metadata::async_server_writes_and_clears_runtime_metadata` and `admin::async_admin_lists_tasks_and_reads_task_info` |

Shutdown/requeue smoke tests must keep explicit server shutdown timeouts and
outer test timeouts so CI does not rely on the upstream default graceful timeout.

## Latest Local Run

- Command: `ASYNQ_RS_REDIS_STRICT=1 cargo test broker::redis::tests:: -- --nocapture --test-threads=1`
- Result: command passed locally with Docker-backed testcontainers:
  `25 passed; 0 failed; 0 ignored`.
- Preflight evidence: `scripts/redis-smoke-preflight.sh --self-test` passes, and
  `scripts/redis-smoke-preflight.sh` passes because Docker daemon access is
  available for testcontainers.
- Gate evidence: `scripts/final-release-gate.sh` passes both release-gate
  passes locally. Each pass runs release shape, metadata, package file-list smoke,
  docs-set, public API, semantic scans, Redis preflight, strict Redis smoke,
  examples, doctests, strict rustdoc, clippy with warnings denied, full tests,
  and diff checks.
- Reliability note: Redis container startup now retries transient
  testcontainers port-resolution failures before strict smoke fails, while
  still preserving strict failure when no Redis URL or Docker path is usable.
