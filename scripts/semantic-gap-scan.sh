#!/usr/bin/env bash
set -euo pipefail

failures=0

check_absent() {
  local description=$1
  local pattern=$2
  shift 2

  if rg -n "$pattern" "$@"; then
    echo "semantic gap scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_absent_multiline() {
  local description=$1
  local pattern=$2
  shift 2

  if rg -U -n "$pattern" "$@"; then
    echo "semantic gap scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_present() {
  local description=$1
  local pattern=$2
  local path=$3

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "semantic gap scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | rg -n "$pattern" - >/dev/null; then
    echo "semantic gap scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_matches_multiline_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | rg -U -n "$pattern" - >/dev/null; then
    echo "semantic gap scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

if [[ "${1:-}" == "--self-test" ]]; then
  check_matches_sample \
    "TODO/FIXME pattern" \
    '\b(TODO|FIXME)\b' \
    '// TODO: document intentional omission before release.'

  check_matches_sample \
    "Known gap marker pattern" \
    'Known gap:' \
    '- Known gap: this should live in alignment-gaps.md instead.'

  check_matches_sample \
    "remaining Processor shell wording pattern" \
    'remaining compatibility shell|compatibility shell still|still owns runtime state' \
    'Known gap: the remaining compatibility shell still owns runtime state.'

  check_matches_sample \
    "legacy processor module path pattern" \
    '\bmod processor;|crate::processor\b|crate::processor::' \
    'use crate::processor::TaskExecutor;'

  check_matches_sample \
    "stale crate-private processor namespace wording pattern" \
    '`processor` is crate-private|crate-private `processor`|processor namespace' \
    'Current audit: `processor` is crate-private.'

  check_matches_sample \
    "stale processor-shaped local runtime variable pattern" \
    '\b(let mut|let) processor(_handle)?\b|processor: &mut WorkerAssembly' \
    'let mut processor = WorkerAssembly::new(...);'

  check_matches_sample \
    "stale processor-owned shutdown wording pattern" \
    'processor shutdown path|processor shutdown/requeue|transitional processor wrapper' \
    'The processor shutdown path requeues unfinished active work.'

  check_matches_sample \
    "stale processor wording scan alias pattern" \
    'processor-wording scan' \
    'Re-run the processor-wording scan before release.'

  check_matches_sample \
    "deleted stale aggregation reclaim alias pattern" \
    '\bReclaimStaleAggregationSetsBroker\b' \
    'pub(crate) trait ReclaimStaleAggregationSetsBroker;'

  check_matches_multiline_sample \
    "worker task runner default no-op pattern" \
    'async fn run_task_once\([^;]*\{[^}]*NoProcessableTask' \
    'async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        Ok(WorkerRun::NoProcessableTask)
    }'

  check_matches_multiline_sample \
    "server shutdown default no-op pattern" \
    'async fn shutdown\([^;]*\{[^}]*Ok\(\(\)\)' \
    'async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        Ok(())
    }'

  check_matches_multiline_sample \
    "server connection default ping no-op pattern" \
    'async fn ping\([^;]*\{[^}]*Ok\(\(\)\)' \
    'async fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }'

  check_matches_multiline_sample \
    "server connection default close no-op pattern" \
    'fn close\([^;]*\{[^}]*Ok\(\(\)\)' \
    'fn close(&mut self) -> Result<(), BrokerError> {
        Ok(())
    }'

  check_matches_multiline_sample \
    "server sync store default no-op pattern" \
    'async fn apply_pending_sync_operation\([^;]*\{[^}]*Ok\(\(\)\)' \
    'async fn apply_pending_sync_operation(
        &mut self,
        _operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        Ok(())
    }'

  check_matches_multiline_sample \
    "server heartbeat metadata default no-op pattern" \
    'async fn (write|clear)_server_metadata\([^;]*\{[^}]*Ok\(\(\)\)' \
    'async fn write_server_metadata(
        &mut self,
        _metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        Ok(())
    }'

  check_matches_multiline_sample \
    "server lease extender default fake extension pattern" \
    'async fn extend_leases\([^;]*\{[^}]*LeaseExtension::new' \
    'async fn extend_leases(
        &mut self,
        _queue: &str,
        _task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        Ok(LeaseExtension::new(SystemTime::now()))
    }'

  check_matches_multiline_sample \
    "server maintenance default empty run pattern" \
    'async fn run_(forwarder|recoverer|janitor)_maintenance\([^;]*\{[^}]*ServerMaintenanceRun::default\(\)' \
    'async fn run_forwarder_maintenance(
        &mut self,
        _queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        Ok(ServerMaintenanceRun::default())
    }'

  check_matches_multiline_sample \
    "server clock default wall-clock pattern" \
    'fn runtime_now\([^;]*\{[^}]*SystemTime::now\(\)' \
    'fn runtime_now(&self) -> SystemTime {
        SystemTime::now()
    }'

  check_matches_multiline_sample \
    "server runtime attach default pass-through pattern" \
    'fn with_server_runtime\([^;]*\{[^}]*self[[:space:]]*\}' \
    'fn with_server_runtime(self, _runtime: &ServerRuntimeState) -> Self {
        self
    }'

  check_matches_sample \
    "legacy server runtime attach builder method pattern" \
    '\bwith_server_runtime\b' \
    'fn with_server_runtime(self, _runtime: &ServerRuntimeState) -> Self { self }'

  check_matches_sample \
    "Redis fixture URL open unwrap pattern" \
    'redis::Client::open\(url\.as_ref\(\)\)\.unwrap\(\)' \
    'let client = redis::Client::open(url.as_ref()).unwrap();'

  check_matches_sample \
    "Redis fixture connection unwrap pattern" \
    'client\.get_connection\(\)\.unwrap\(\)' \
    'let connection = client.get_connection().unwrap();'

  check_matches_multiline_sample \
    "aggregation group discovery default empty-list pattern" \
    'async fn list_aggregation_groups\([^;]*\{[^}]*Ok\(Vec::new\(\)\)' \
    'async fn list_aggregation_groups(
        &mut self,
        _queue: &str,
    ) -> Result<Vec<String>, AggregationError> {
        Ok(Vec::new())
    }'

  check_matches_sample \
    "server runtime ownership heading pattern" \
    '^### Server Runtime Ownership$' \
    '### Server Runtime Ownership'

  check_matches_sample \
    "Redis write timeout heading pattern" \
    '^### Redis Write Timeout$' \
    '### Redis Write Timeout'

  check_matches_sample \
    "TLS config heading pattern" \
    '^### TLS Config$' \
    '### TLS Config'

  if (( failures > 0 )); then
    exit 1
  fi

  echo "semantic gap scan self-test passed"
  exit 0
fi

check_absent \
  "source must not carry unresolved TODO/FIXME items" \
  '\b(TODO|FIXME)\b' \
  src

check_absent \
  "active docs/source must not use scattered Known gap markers" \
  'Known gap:' \
  docs README.md src \
  --glob '!docs/release-readiness-roadmap.md'

check_absent \
  "active docs/source must not describe a remaining Processor runtime shell" \
  'remaining compatibility shell|compatibility shell still|still owns runtime state' \
  docs README.md src

check_absent \
  "active docs/source must not reintroduce the legacy processor module path" \
  '\bmod processor;|crate::processor\b|crate::processor::' \
  docs README.md src

check_absent \
  "active docs/source must not describe a current crate-private processor namespace" \
  '`processor` is crate-private|crate-private `processor`|processor namespace' \
  docs README.md src

check_absent \
  "source must not use processor-shaped local runtime variable names" \
  '\b(let mut|let) processor(_handle)?\b|processor: &mut WorkerAssembly' \
  src \
  --glob '!src/pb/**'

check_absent \
  "source must not describe current shutdown/runtime ownership as processor-owned" \
  'processor shutdown path|processor shutdown/requeue|transitional processor wrapper' \
  src \
  --glob '!src/pb/**'

check_absent \
  "active docs/source must name the semantic gap scan instead of stale processor wording scan aliases" \
  'processor-wording scan' \
  docs README.md src

check_absent \
  "active docs/source must not reintroduce the deleted server-only stale aggregation reclaim alias" \
  '\bReclaimStaleAggregationSetsBroker\b' \
  docs README.md src

check_absent_multiline \
  "source must not give WorkerTaskRunner a default no-op run_task_once implementation" \
  'async fn run_task_once\([^;]*\{[^}]*NoProcessableTask' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerShutdown a default no-op shutdown implementation" \
  'async fn shutdown\([^;]*\{[^}]*Ok\(\(\)\)' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerConnection a default no-op ping implementation" \
  'async fn ping\([^;]*\{[^}]*Ok\(\(\)\)' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerConnection a default no-op close implementation" \
  'fn close\([^;]*\{[^}]*Ok\(\(\)\)' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerSyncStore a default no-op pending-sync implementation" \
  'async fn apply_pending_sync_operation\([^;]*\{[^}]*Ok\(\(\)\)' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerHeartbeatStore default no-op metadata implementations" \
  'async fn (write|clear)_server_metadata\([^;]*\{[^}]*Ok\(\(\)\)' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerLeaseExtender a default fake lease extension implementation" \
  'async fn extend_leases\([^;]*\{[^}]*LeaseExtension::new' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerMaintenanceRunner default empty maintenance implementations" \
  'async fn run_(forwarder|recoverer|janitor)_maintenance\([^;]*\{[^}]*ServerMaintenanceRun::default\(\)' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerClock a default wall-clock implementation" \
  'fn runtime_now\([^;]*\{[^}]*SystemTime::now\(\)' \
  src/server/worker.rs

check_absent_multiline \
  "source must not give ServerRuntimeStateAttach a default pass-through implementation" \
  'fn with_server_runtime\([^;]*\{[^}]*self[[:space:]]*\}' \
  src/server/worker.rs

check_absent \
  "source must not reintroduce legacy builder-style server runtime attach methods" \
  '\bwith_server_runtime\b' \
  src

check_absent_multiline \
  "source must not reintroduce client test-only broker/shared accessors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (broker|has_shared_connection)\(' \
  src/client/model.rs

check_absent_multiline \
  "source must not reintroduce scheduler test-only broker/shared accessors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (broker|has_shared_connection)\(' \
  src/scheduler/core/accessors.rs

check_absent_multiline \
  "source must not reintroduce scheduler test-only configuration accessors" \
  '#\[cfg\(test\)\][[:space:]]+pub(\(crate\))? fn (scheduler_id|entries|state|tick_interval|heartbeat_interval|metadata_ttl|timezone)\(' \
  src/scheduler/core/accessors.rs

check_absent_multiline \
  "worker assembly test helper must not reintroduce a broker getter" \
  'fn broker\(&self\) -> &B' \
  src/server/worker_assembly_test_helpers.rs

check_absent_multiline \
  "source must not reintroduce server test-only configuration accessors" \
  '#\[cfg\(test\)\][[:space:]]+pub fn (queues|worker_count|queue_selector|idle_sleep|forwarder_interval|recoverer_interval|janitor_interval|syncer_interval|shutdown_timeout|health_check_interval|aggregation_config|metadata|metadata_heartbeat_interval|has_aggregation_runner|has_health_check_handler|log_level|logger)\(' \
  src/server/accessors.rs

check_absent_multiline \
  "source must not reintroduce server builder test-only interval setters" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn with_(maintenance|recoverer|syncer)_interval\(' \
  src/server/builder/intervals.rs

check_absent_multiline \
  "source must not reintroduce server builder test-only logger setters" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn with_logger\(' \
  src/server/builder/logging.rs

check_absent_multiline \
  "source must not reintroduce server builder test-only aggregation inspectors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn aggregation_group_configs\(' \
  src/server/builder/aggregation.rs

check_absent_multiline \
  "source must not reintroduce server builder test-only health handlers" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn with_health_check_(handler|func)\(' \
  src/server/builder/health.rs

check_absent_multiline \
  "source must not reintroduce server runtime state test-only helpers" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (with_active_worker|with_pending_complete|pending_sync_count)\(' \
  src/server/runtime_state.rs

check_absent_multiline \
  "metadata codec must not expose cfg-test timestamp helpers" \
  '#\[cfg\(test\)\][[:space:]]+pub\(in crate::server\) fn timestamp\(' \
  src/server/metadata/codec.rs

check_absent_multiline \
  "result writer must not expose cfg-test channel constructors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn channel\(' \
  src/operation/lifecycle/result.rs

check_absent_multiline \
  "Redis cancel listener must not expose cfg-test canceller accessors" \
  '#\[cfg\(test\)\][[:space:]]+pub fn canceller\(' \
  src/broker/redis/listener.rs

check_absent_multiline \
  "Redis parse time must not expose cfg-test timestamp constructors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(in crate::broker::redis::broker\) fn system_time_from_unix_seconds_and_nanoseconds\(' \
  src/broker/redis/broker/parse/time.rs

check_absent_multiline \
  "parallel server runtime must not expose cfg-test run entry points on Server" \
  '#\[cfg\(test\)\][[:space:]]+(pub\(crate\) )?async fn run_until_stopped_(configured_parallel|parallel)' \
  src/server/run/parallel.rs

check_absent_multiline \
  "single server runtime must not expose cfg-test run entry points on Server" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) async fn run_until_stopped\(' \
  src/server/run/single.rs

check_absent \
  "server state must not carry cfg-test lifecycle variants" \
  '#\[cfg\(test\)\]' \
  src/server/state.rs

check_absent_multiline \
  "server canceller must not expose test-only active task registry snapshots" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn active_task_ids\(' \
  src/server/cancellation.rs

check_absent_multiline \
  "pending sync module must not attach cfg-test worker assembly helpers" \
  '#\[cfg\(test\)\][[:space:]]+impl<[^>]+> WorkerAssembly' \
  src/server/pending_sync.rs

check_absent_multiline \
  "source must not reintroduce server test-only constructors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (with_sleeper|with_weighted_queues|with_strict_priority_queues|with_queue_selector|new|new_with_config)\(' \
  src/server/constructors.rs

check_absent_multiline \
  "source must not reintroduce Redis-backed server builder test-only helpers" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) (async )?fn (has_shared_connection|build_with_optional_handler|run_optional|start_optional)\(' \
  src/server/constructors/redis.rs

check_absent \
  "server builder must not restore the deleted test-only metadata builder module" \
  '^mod metadata;' \
  src/server/builder.rs

check_absent \
  "worker assembly test support must keep cfg(test) at the module boundary" \
  '#\[cfg\(test\)\]' \
  src/server/worker_assembly_config

check_absent_multiline \
  "source must not reintroduce inspector test-only broker/shared accessors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (broker|has_shared_connection)\(' \
  src/admin/inspector.rs

check_absent_multiline \
  "source must not reintroduce inspector test-only constructors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (new|with_shared_connection)\(' \
  src/admin/inspector.rs

check_absent_multiline \
  "source must not reintroduce aggregator test-only configuration/accessor methods" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (with_logger|with_tick_interval|add_group|tick_interval|broker|handler)\(' \
  src/aggregation/mod.rs

check_absent_multiline \
  "source must not reintroduce aggregator test-only run_once method" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) async fn run_once\(' \
  src/aggregation/runtime/run_loop.rs

check_absent_multiline \
  "source must not reintroduce aggregation model test-only accessors" \
  '#\[cfg\(test\)\][[:space:]]+pub\(crate\) fn (messages|checked|aggregated|reclaimed|skipped)\(' \
  src/aggregation/models.rs

check_absent \
  "Redis plan module must not restore a module-wide dead-code allowance" \
  '#!\[allow\(dead_code\)\]' \
  src/broker/redis/plan/mod.rs

check_absent \
  "Redis fixture must not unwrap configured URL open without strict diagnostic" \
  'redis::Client::open\(url\.as_ref\(\)\)\.unwrap\(\)' \
  src/broker/redis/tests/fixture.rs

check_absent \
  "Redis fixture must not unwrap configured URL connection without strict diagnostic" \
  'client\.get_connection\(\)\.unwrap\(\)' \
  src/broker/redis/tests/fixture.rs

check_present \
  "Redis fixture must keep configured URL open diagnostic" \
  'failed to open configured Redis URL from' \
  src/broker/redis/tests/fixture.rs

check_present \
  "Redis fixture must keep configured URL connection diagnostic" \
  'failed to connect to configured Redis URL from' \
  src/broker/redis/tests/fixture.rs

check_absent_multiline \
  "source must not give AggregationBroker a default empty group discovery implementation" \
  'async fn list_aggregation_groups\([^;]*\{[^}]*Ok\(Vec::new\(\)\)' \
  src/aggregation/broker.rs

check_present \
  "alignment gaps must track server runtime ownership" \
  '^### Server Runtime Ownership$' \
  docs/alignment-gaps.md

check_present \
  "alignment gaps must track Redis write timeout decision" \
  '^### Redis Write Timeout$' \
  docs/alignment-gaps.md

check_present \
  "alignment gaps must track TLS config decision" \
  '^### TLS Config$' \
  docs/alignment-gaps.md

if (( failures > 0 )); then
  exit 1
fi

echo "semantic gap scan passed"
