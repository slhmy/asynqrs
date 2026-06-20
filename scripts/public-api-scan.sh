#!/usr/bin/env bash
set -euo pipefail

failures=0

check_absent() {
  local description=$1
  local pattern=$2
  shift 2

  if rg -n "$pattern" "$@"; then
    echo "public API scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_absent_multiline() {
  local description=$1
  local pattern=$2
  shift 2

  if rg -U -n "$pattern" "$@"; then
    echo "public API scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | rg -n "$pattern" - >/dev/null; then
    echo "public API scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_matches_multiline_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | rg -U -n "$pattern" - >/dev/null; then
    echo "public API scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_prelude_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | awk '
    /^pub mod prelude \{/ { in_prelude = 1 }
    in_prelude { print }
    in_prelude && /^}$/ { exit }
  ' | rg -n "$pattern" - >/dev/null; then
    echo "public API scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_processing_export_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | awk '
    /^pub use processing::\{/ { in_processing_export = 1 }
    in_processing_export { print }
    in_processing_export && /^};$/ { exit }
  ' | rg -n "$pattern" - >/dev/null; then
    echo "public API scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_crate_root_export_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | awk '
    /^pub mod prelude \{/ { exit }
    /^pub use / { in_export = 1 }
    in_export { print }
    in_export && /;$/ { in_export = 0 }
  ' | rg -n "$pattern" - >/dev/null; then
    echo "public API scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_present() {
  local description=$1
  local pattern=$2
  local path=$3

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "public API scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_crate_root_export_present() {
  local description=$1
  local pattern=$2

  if ! awk '
    /^pub mod prelude \{/ { exit }
    /^pub use / { in_export = 1 }
    in_export { print }
    in_export && /;$/ { in_export = 0 }
  ' src/lib.rs | rg -n "$pattern" - >/dev/null; then
    echo "public API scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_prelude_present() {
  local description=$1
  local pattern=$2

  if ! awk '
    /^pub mod prelude \{/ { in_prelude = 1 }
    in_prelude { print }
    in_prelude && /^}$/ { exit }
  ' src/lib.rs | rg -n "$pattern" - >/dev/null; then
    echo "public API scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_processing_export_present() {
  local description=$1
  local pattern=$2

  if ! awk '
    /^pub use processing::\{/ { in_processing_export = 1 }
    in_processing_export { print }
    in_processing_export && /^};$/ { exit }
  ' src/lib.rs | rg -n "$pattern" - >/dev/null; then
    echo "public API scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

if [[ "${1:-}" == "--self-test" ]]; then
  check_matches_sample \
    "processing public module pattern" \
    '^pub mod processing\b' \
    'pub mod processing;'

  check_matches_sample \
    "crate-root internal runtime/state re-export pattern" \
    '^pub use .*\b(ServerState|SchedulerState|AggregatorRun|WorkerAssembly|WorkerRun|ServerRuntimeState|ProcessingLease|PendingSyncDrainPolicy|PendingSyncOperation)\b' \
    'pub use server::ServerState;'

  check_matches_sample \
    "crate-root crate-private re-export pattern" \
    '^pub\(crate\) use\b' \
    'pub(crate) use server::ServerRuntimeState;'

  check_matches_multiline_sample \
    "test-only public facade pattern" \
    '#\[cfg\(test\)\][[:space:]]*pub(\([^)]*\))?[[:space:]]+(use|mod)[[:space:]]' \
    $'#[cfg(test)]\npub(crate) use worker::WorkerTaskRunner;'

  check_matches_multiline_sample \
    "test-only public module facade pattern" \
    '#\[cfg\(test\)\][[:space:]]*pub(\([^)]*\))?[[:space:]]+(use|mod)[[:space:]]' \
    $'#[cfg(test)]\npub mod redis_test_facade;'

  check_matches_sample \
    "crate-root internal broker trait re-export pattern" \
    '^pub use .*\b(Broker|AsyncBroker|CloseBroker|SchedulerBroker|AggregationBroker|CancelBroker|InspectorBulkBroker|InspectorMetadataBroker|InspectorQueueBroker|InspectorStatsBroker|InspectorTaskBroker|InspectorTaskReadBroker|WorkerBrokerCore|RetryBroker|ArchiveBroker|RequeueBroker|LeaseBroker|ForwardBroker|RecoverBroker|ReclaimStaleAggregationSetsBroker|CleanupBroker|MetadataBroker|PingBroker)\b' \
    'pub use server::WorkerBrokerCore;'

  check_matches_sample \
    "internal module public export pattern" \
    '^pub mod (broker|pb|operation|compat|signal)\b' \
    'pub mod broker;'

  check_matches_sample \
    "server internal runtime assembly re-export pattern" \
    '^pub use .*\b(ServerRuntimeState|ServerState|WorkerAssembly|WorkerRun|ProcessingLease|WorkerRuntimeParts)\b' \
    'pub use runtime_state::ServerRuntimeState;'

  check_matches_sample \
    "public config field pattern" \
    '^    pub [A-Za-z_][A-Za-z0-9_]*:' \
    '    pub concurrency: isize,'

  check_matches_sample \
    "redis-backed server runtime probe method pattern" \
    '^    pub (async )?fn (has_shared_connection|broker|broker_mut|into_broker|config|queues|worker_count|runtime_state|metadata)\b' \
    '    pub fn runtime_state(&self) -> ServerRuntimeState {'

  check_matches_sample \
    "scheduler state re-export pattern" \
    '^pub use .*\bSchedulerState\b' \
    'pub use state::SchedulerState;'

  check_matches_sample \
    "scheduler broker module extension point pattern" \
    '^pub use broker::SchedulerBroker;' \
    'pub use broker::SchedulerBroker;'

  check_matches_sample \
    "client broker module extension point pattern" \
    '^pub use traits::\{AsyncBroker, Broker, CloseBroker\};' \
    'pub use traits::{AsyncBroker, Broker, CloseBroker};'

  check_matches_sample \
    "crate-root processing customization export pattern" \
    '\b(Handler|ServeMux|TaskMiddlewareHooks|task_middleware_hooks)\b' \
    'pub use processing::{Handler, ServeMux, TaskMiddlewareHooks, task_middleware_hooks};'

  check_crate_root_export_matches_sample \
    "scoped crate-root workflow export pattern" \
    '\bRedisBackedClient\b' \
    '//! RedisBackedClient in docs.
pub use client::{
    RedisBackedClient,
};
pub mod prelude {
    pub use crate::Task;
}'

  if printf '%s\n' '//! RedisBackedClient in docs.
pub mod prelude {
    pub use crate::RedisBackedClient;
}' | awk '
    /^pub mod prelude \{/ { exit }
    /^pub use / { in_export = 1 }
    in_export { print }
    in_export && /;$/ { in_export = 0 }
  ' | rg -n '\bRedisBackedClient\b' - >/dev/null; then
    echo "public API scan self-test failed: crate-root export guard accepted docs/prelude-only workflow wording" >&2
    failures=$((failures + 1))
  fi

  check_processing_export_matches_sample \
    "scoped processing customization export pattern" \
    '\bServeMux\b' \
    '//! use ServeMux in examples.
pub use processing::{
    Handler, ServeMux,
};'

  if printf '%s\n' '//! use ServeMux in examples.
pub use task::Task;' | awk '
    /^pub use processing::\{/ { in_processing_export = 1 }
    in_processing_export { print }
    in_processing_export && /^};$/ { exit }
  ' | rg -n '\bServeMux\b' - >/dev/null; then
    echo "public API scan self-test failed: processing export guard accepted docs-only ServeMux wording" >&2
    failures=$((failures + 1))
  fi

  check_matches_sample \
    "prelude ServeMux workflow export pattern" \
    '\bServeMux, ServerError\b' \
    'pub use crate::{Handler, ServeMux, ServerError, Task};'

  check_matches_sample \
    "prelude block extraction opening pattern" \
    '^pub mod prelude \{$' \
    'pub mod prelude {'

  check_prelude_matches_sample \
    "prelude scoped export pattern" \
    '\bQueueInfo\b' \
    'pub use crate::TaskInfo;
pub mod prelude {
    pub use crate::{QueueInfo, Task};
}'

  if printf '%s\n' 'pub use crate::QueueInfo;
pub mod prelude {
    pub use crate::Task;
}' | awk '
    /^pub mod prelude \{/ { in_prelude = 1 }
    in_prelude { print }
    in_prelude && /^}$/ { exit }
  ' | rg -n '\bQueueInfo\b' - >/dev/null; then
    echo "public API scan self-test failed: prelude scoped export guard accepted a crate-root-only export" >&2
    failures=$((failures + 1))
  fi

  check_matches_sample \
    "prelude aggregation workflow export pattern" \
    '\bGroupAggregator, GroupAggregatorFunc\b' \
    'pub use crate::{GroupAggregator, GroupAggregatorFunc, Task};'

  check_matches_sample \
    "prelude middleware hooks workflow export pattern" \
    '\bTaskMiddlewareHooks\b' \
    'pub use crate::{TaskMiddlewareHooks, TaskState, TaskType};'

  check_matches_sample \
    "prelude processing scope workflow export pattern" \
    '\bProcessingContext, ProcessingScope, ServerProcessingScope, TaskMetadata\b' \
    'pub use crate::{ProcessingContext, ProcessingScope, ServerProcessingScope, TaskMetadata};'

  check_matches_sample \
    "prelude periodic workflow export pattern" \
    '\bRedisBackedPeriodicTaskManager\b' \
    'pub use crate::{PeriodicTaskConfig, PeriodicTaskManager, RedisBackedPeriodicTaskManager};'

  check_matches_sample \
    "prelude inspector read model workflow export pattern" \
    '\bQueueInfo\b' \
    'pub use crate::{Inspector, QueueInfo, TaskInfo, ServerInfo, WorkerInfo};'

  check_matches_sample \
    "admin module typed inspector workflow export pattern" \
    '^pub use inspector::\{Inspector, InspectorError\};' \
    'pub use inspector::{Inspector, InspectorError};'

  check_matches_sample \
    "admin private inspector module pattern" \
    '^pub mod inspector\b' \
    'pub mod inspector;'

  check_matches_sample \
    "admin private cancellation module pattern" \
    '^pub mod cancellation\b' \
    'pub mod cancellation;'

  check_matches_sample \
    "admin public cancellation broker re-export pattern" \
    '^pub use cancellation::CancelBroker\b|^pub use .*CancelBroker\b' \
    'pub use cancellation::CancelBroker;'

  check_matches_sample \
    "admin inspector implementation submodule public export pattern" \
    '^pub mod (bulk|metadata|task_read)\b' \
    'pub mod bulk;'

  check_matches_sample \
    "admin inspector broker submodule public re-export pattern" \
    '^pub use (bulk::InspectorBulkBroker|metadata::InspectorMetadataBroker|task_read::InspectorTaskReadBroker)\b' \
    'pub use bulk::InspectorBulkBroker;'

  check_matches_sample \
    "crate-root admin metadata codec helper export pattern" \
    '\b(decode_server_info|encode_server_info|decode_worker_info|encode_worker_info|decode_scheduler_entry|encode_scheduler_entry|decode_scheduler_enqueue_event|encode_scheduler_enqueue_event)\b' \
    'pub use admin::{decode_server_info, encode_server_info};'

  check_matches_sample \
    "admin module public metadata codec helper export pattern" \
    '^pub use codec::\{[^;]*(decode_server_info|encode_server_info|decode_worker_info|encode_worker_info|decode_scheduler_entry|encode_scheduler_entry|decode_scheduler_enqueue_event|encode_scheduler_enqueue_event)' \
    'pub use codec::{decode_server_info, encode_server_info};'

  check_matches_sample \
    "aggregation run re-export pattern" \
    '^pub use .*\bAggregatorRun\b' \
    'pub use models::AggregatorRun;'

  check_matches_sample \
    "aggregation private broker module pattern" \
    '^pub mod broker\b' \
    'pub mod broker;'

  check_matches_sample \
    "aggregation broker public re-export pattern" \
    '^pub use .*AggregationBroker\b' \
    'pub use broker::AggregationBroker;'

  if (( failures > 0 )); then
    exit 1
  fi

  echo "public API scan self-test passed"
  exit 0
fi

check_absent \
  "processing module must stay crate-private" \
  '^pub mod processing\b' \
  src/lib.rs

check_processing_export_present \
  "crate root must expose Handler as a processing customization workflow type" \
  '\bHandler\b'

check_processing_export_present \
  "crate root must expose ServeMux as the preferred handler routing workflow" \
  '\bServeMux\b'

check_prelude_present \
  "prelude must expose ServeMux as the preferred handler routing workflow" \
  '\bServeMux\b'

check_prelude_present \
  "prelude must expose ServerError as the preferred server workflow error" \
  '\bServerError\b'

check_prelude_present \
  "prelude must expose GroupAggregator as the preferred aggregation workflow" \
  '\bGroupAggregator\b'

check_prelude_present \
  "prelude must expose GroupAggregatorFunc as the preferred aggregation workflow" \
  '\bGroupAggregatorFunc\b'

check_prelude_present \
  "prelude must expose TaskMiddlewareHooks as before/after middleware hook customization workflow" \
  '\bTaskMiddlewareHooks\b'

check_prelude_present \
  "prelude must expose task_middleware_hooks as before/after middleware hook customization workflow" \
  '\btask_middleware_hooks\b'

check_prelude_present \
  "prelude must expose ProcessingScope as a handler scope customization workflow type" \
  '\bProcessingScope\b'

check_prelude_present \
  "prelude must expose ServerProcessingScope as a handler scope customization workflow type" \
  '\bServerProcessingScope\b'

check_prelude_present \
  "prelude must expose TaskMetadata as a handler metadata workflow type" \
  '\bTaskMetadata\b'

check_prelude_present \
  "prelude must expose RedisBackedPeriodicTaskManager as the preferred periodic workflow" \
  '\bRedisBackedPeriodicTaskManager\b'

check_prelude_present \
  "prelude must expose PeriodicTaskManager as the advanced periodic workflow" \
  '\bPeriodicTaskManager\b'

check_prelude_present \
  "prelude must expose PeriodicTaskConfig for periodic task configuration" \
  '\bPeriodicTaskConfig\b'

check_prelude_present \
  "prelude must expose QueueInfo as an inspector read model" \
  '\bQueueInfo\b'

check_prelude_present \
  "prelude must expose TaskInfo as an inspector read model" \
  '\bTaskInfo\b'

check_prelude_present \
  "prelude must expose ServerInfo as an inspector read model" \
  '\bServerInfo\b'

check_prelude_present \
  "prelude must expose WorkerInfo as an inspector read model" \
  '\bWorkerInfo\b'

check_prelude_present \
  "prelude must expose SchedulerEntryInfo as an inspector read model" \
  '\bSchedulerEntryInfo\b'

check_prelude_present \
  "prelude must expose SchedulerEnqueueEventInfo as an inspector read model" \
  '\bSchedulerEnqueueEventInfo\b'

check_processing_export_present \
  "crate root must expose TaskMiddlewareHooks for before/after middleware customization" \
  '\bTaskMiddlewareHooks\b'

check_processing_export_present \
  "crate root must expose task_middleware_hooks for before/after middleware customization" \
  '\btask_middleware_hooks\b'

check_crate_root_export_present \
  "crate root must expose RedisBackedClient as the preferred enqueue workflow" \
  '\bRedisBackedClient\b'

check_crate_root_export_present \
  "crate root must expose EnqueueOptions as the preferred enqueue options workflow" \
  '\bEnqueueOptions\b'

check_crate_root_export_present \
  "crate root must expose RedisBackedServerBuilder as the preferred server workflow" \
  '\bRedisBackedServerBuilder\b'

check_crate_root_export_present \
  "crate root must expose Config as the preferred server configuration workflow" \
  '\bConfig\b'

check_crate_root_export_present \
  "crate root must expose RedisBackedScheduler as the preferred scheduler workflow" \
  '\bRedisBackedScheduler\b'

check_crate_root_export_present \
  "crate root must expose Inspector as the preferred inspector workflow" \
  '\bInspector\b'

check_crate_root_export_present \
  "crate root must expose GroupAggregator as the preferred aggregation workflow" \
  '\bGroupAggregator\b'

check_crate_root_export_present \
  "crate root must expose Task as the core task model workflow" \
  '\bTask\b'

check_absent \
  "crate root must not re-export internal runtime/state types" \
  '^pub use .*\b(ServerState|SchedulerState|AggregatorRun|WorkerAssembly|WorkerRun|ServerRuntimeState|ProcessingLease|PendingSyncDrainPolicy|PendingSyncOperation)\b' \
  src/lib.rs

check_absent \
  "crate root must stay a public API facade without crate-private re-exports" \
  '^pub\(crate\) use\b' \
  src/lib.rs

check_absent_multiline \
  "source must not hide public or crate-public facades behind cfg(test)" \
  '#\[cfg\(test\)\][[:space:]]*pub(\([^)]*\))?[[:space:]]+(use|mod)[[:space:]]' \
  src

check_absent \
  "crate root must not re-export internal broker traits" \
  '^pub use .*\b(Broker|AsyncBroker|CloseBroker|SchedulerBroker|AggregationBroker|CancelBroker|InspectorBulkBroker|InspectorMetadataBroker|InspectorQueueBroker|InspectorStatsBroker|InspectorTaskBroker|InspectorTaskReadBroker|WorkerBrokerCore|RetryBroker|ArchiveBroker|RequeueBroker|LeaseBroker|ForwardBroker|RecoverBroker|ReclaimStaleAggregationSetsBroker|CleanupBroker|MetadataBroker|PingBroker)\b' \
  src/lib.rs

check_absent \
  "crate root must not re-export admin metadata codec helpers" \
  '\b(decode_server_info|encode_server_info|decode_worker_info|encode_worker_info|decode_scheduler_entry|encode_scheduler_entry|decode_scheduler_enqueue_event|encode_scheduler_enqueue_event)\b' \
  src/lib.rs

check_absent \
  "crate root must not expose internal implementation modules" \
  '^pub mod (broker|pb|operation|compat|signal)\b' \
  src/lib.rs

check_present \
  "admin module must expose Inspector as the typed inspector workflow" \
  '^pub use inspector::\{Inspector, InspectorError\};' \
  src/admin/mod.rs

check_absent \
  "admin module must keep inspector implementation module private" \
  '^pub mod inspector\b' \
  src/admin/mod.rs

check_absent \
  "admin module must keep cancellation implementation module private" \
  '^pub mod cancellation\b' \
  src/admin/mod.rs

check_absent \
  "admin module must not publicly re-export inspector broker traits" \
  '^pub use inspector::\{.*Inspector.*Broker|^pub use inspector::Inspector.*Broker' \
  src/admin/mod.rs

check_absent \
  "admin module must not publicly re-export cancellation broker traits" \
  '^pub use cancellation::CancelBroker\b|^pub use .*CancelBroker\b' \
  src/admin/mod.rs

check_absent \
  "admin module must not publicly re-export metadata codec helpers" \
  '^pub use codec::\{[^;]*(decode_server_info|encode_server_info|decode_worker_info|encode_worker_info|decode_scheduler_entry|encode_scheduler_entry|decode_scheduler_enqueue_event|encode_scheduler_enqueue_event)' \
  src/admin/mod.rs

check_absent \
  "inspector module must keep implementation submodules private" \
  '^pub mod (bulk|metadata|task_read)\b' \
  src/admin/inspector.rs

check_absent \
  "inspector module must not publicly re-export broker submodule traits" \
  '^pub use (bulk::InspectorBulkBroker|metadata::InspectorMetadataBroker|task_read::InspectorTaskReadBroker)\b' \
  src/admin/inspector.rs

check_absent \
  "server module must not publicly re-export internal runtime assembly types" \
  '^pub use .*\b(ServerRuntimeState|ServerState|WorkerAssembly|WorkerRun|ProcessingLease|WorkerRuntimeParts)\b' \
  src/server.rs

check_absent \
  "Config fields must stay private runtime state behind builder/accessors" \
  '^    pub [A-Za-z_][A-Za-z0-9_]*:' \
  src/server/config.rs

check_absent \
  "RedisBackedServer public wrapper must not expose runtime/config probes" \
  '^    pub (async )?fn (has_shared_connection|broker|broker_mut|into_broker|config|queues|worker_count|runtime_state|metadata)\b' \
  src/server/constructors/redis.rs

check_absent \
  "scheduler module must not publicly re-export SchedulerState" \
  '^pub use .*\bSchedulerState\b' \
  src/scheduler.rs

check_present \
  "scheduler module must publish SchedulerBroker as the documented custom backend extension point" \
  '^pub use broker::SchedulerBroker;' \
  src/scheduler.rs

check_present \
  "client module must publish broker traits as the documented custom enqueue backend extension point" \
  '^pub use traits::\{AsyncBroker, Broker, CloseBroker\};' \
  src/client.rs

check_absent \
  "aggregation module must not publicly re-export AggregatorRun" \
  '^pub use .*\bAggregatorRun\b' \
  src/aggregation/mod.rs

check_absent \
  "aggregation module must keep broker implementation module private" \
  '^pub mod broker\b' \
  src/aggregation/mod.rs

check_absent \
  "aggregation module must not publicly re-export AggregationBroker" \
  '^pub use .*AggregationBroker\b' \
  src/aggregation/mod.rs

if (( failures > 0 )); then
  exit 1
fi

echo "public API scan passed"
