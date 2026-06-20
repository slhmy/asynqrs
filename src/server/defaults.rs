//! Server default timing constants.
//!
//! Reference: Asynq v0.26.0 server default constants:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L410-L424>.

use std::time::Duration;

/// Default interval between empty queue polls.
///
/// Reference: Asynq v0.26.0 `DEFAULT_SERVER_IDLE_SLEEP`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L410>.
pub const DEFAULT_SERVER_IDLE_SLEEP: Duration = Duration::from_secs(1);
/// Default interval for moving due scheduled/retry tasks into pending state.
///
/// Reference: Asynq v0.26.0 `DEFAULT_SERVER_FORWARDER_INTERVAL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L420>.
pub const DEFAULT_SERVER_FORWARDER_INTERVAL: Duration = Duration::from_secs(5);
/// Default interval for recovering expired worker leases.
///
/// Reference: Asynq v0.26.0 recoverer interval:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L561-L568>.
pub const DEFAULT_SERVER_RECOVERER_INTERVAL: Duration = Duration::from_secs(60);
/// Default interval for deleting expired completed tasks.
///
/// Reference: Asynq v0.26.0 `DEFAULT_SERVER_JANITOR_INTERVAL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L424>.
pub const DEFAULT_SERVER_JANITOR_INTERVAL: Duration = Duration::from_secs(8);
/// Default number of expired completed tasks deleted in one janitor pass.
///
/// Reference: Asynq v0.26.0 `DEFAULT_JANITOR_BATCH_SIZE`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L426>.
pub(crate) const DEFAULT_JANITOR_BATCH_SIZE: isize = 100;
/// Compatibility alias for the default forwarder maintenance interval.
///
/// Reference: Asynq v0.26.0 delayed-task check interval:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L420>.
pub const DEFAULT_SERVER_MAINTENANCE_INTERVAL: Duration = DEFAULT_SERVER_FORWARDER_INTERVAL;
pub const DEFAULT_SERVER_RECOVER_RETRY_DELAY: Duration = Duration::from_secs(60);
/// Default interval for retrying failed worker lifecycle writes.
///
/// Reference: Asynq v0.26.0 syncer runs alongside the processor to retry
/// failed complete, retry, and archive writes:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.
pub const DEFAULT_SERVER_SYNCER_INTERVAL: Duration = Duration::from_secs(5);
/// Default duration to wait for in-flight workers during shutdown.
///
/// Reference: Asynq v0.26.0 `DEFAULT_SERVER_SHUTDOWN_TIMEOUT`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L412>.
pub const DEFAULT_SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);
/// Default interval between broker health checks when a health-check handler is configured.
///
/// Reference: Asynq v0.26.0 `DEFAULT_SERVER_HEALTH_CHECK_INTERVAL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L418>.
pub const DEFAULT_SERVER_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(15);
/// Default interval between server heartbeat metadata writes.
///
/// Reference: Asynq v0.26.0 `newHeartbeater` interval:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L495-L505>.
pub const DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// Default TTL for server heartbeat metadata.
///
/// Reference: Asynq v0.26.0 heartbeater writes server state with
/// `interval*2` TTL:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L143-L154>.
pub const DEFAULT_SERVER_METADATA_TTL: Duration =
    DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL.saturating_mul(2);
pub const DEFAULT_SERVER_GROUP_GRACE_PERIOD: Duration = Duration::from_secs(60);
