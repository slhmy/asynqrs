use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::logging::log_debug;
use super::{
    DEFAULT_SERVER_HEALTH_CHECK_INTERVAL, LogLevel, Logger, ServerConnection, ServerError,
};

/// Receives periodic server broker health-check results.
///
/// Reference: Asynq v0.26.0 `Config.HealthCheckFunc` receives each broker
/// ping result from the server-owned healthchecker:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L204-L211>,
/// <https://github.com/hibiken/asynq/blob/v0.26.0/healthcheck.go#L58-L84>.
pub trait HealthCheckHandler: Send + Sync + fmt::Debug {
    fn handle(&self, result: Result<(), String>);
}

/// Adapter for function-style health check callbacks.
///
/// Reference: Asynq v0.26.0 public `Config.HealthCheckFunc` field:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L204-L211>.
///
/// Rust compatibility: `Config` stores this as a trait object instead of a
/// direct function-typed field.
pub struct HealthCheckFunc<F>(pub F);

impl<F> fmt::Debug for HealthCheckFunc<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HealthCheckFunc").finish_non_exhaustive()
    }
}

impl<F> HealthCheckFunc<F>
where
    F: Fn(Result<(), String>) + Send + Sync,
{
    /// Calls the wrapped health-check callback.
    ///
    /// Reference: Asynq v0.26.0 public `Config.HealthCheckFunc` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L204-L211>.
    pub fn handle(&self, result: Result<(), String>) {
        (self.0)(result);
    }
}

impl<F> HealthCheckHandler for HealthCheckFunc<F>
where
    F: Fn(Result<(), String>) + Send + Sync,
{
    fn handle(&self, result: Result<(), String>) {
        HealthCheckFunc::handle(self, result);
    }
}

pub(super) async fn run_health_checks<P>(
    connection: &mut P,
    handler: Arc<dyn HealthCheckHandler>,
    health_check_interval: Duration,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError>
where
    P: ServerConnection + Send,
{
    let mut health_check =
        tokio::time::interval(effective_health_check_interval(health_check_interval));
    health_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    health_check.tick().await;
    while !*shutdown.borrow() {
        tokio::select! {
            _ = health_check.tick() => {
                run_health_check(connection, &handler).await;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    // Reference: Asynq v0.26.0 healthchecker logs after receiving shutdown:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/healthcheck.go#L55-L60>.
    log_debug(logger, log_level, "Healthchecker done");
    Ok(())
}

pub(super) async fn run_health_check<P>(connection: &mut P, handler: &Arc<dyn HealthCheckHandler>)
where
    P: ServerConnection + Send,
{
    // Reference: Asynq v0.26.0 healthchecker periodically calls Broker.Ping
    // and passes the error, if any, to the configured HealthCheckFunc:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/healthcheck.go#L66-L84>.
    handler.handle(connection.ping().await);
}

pub(super) fn effective_health_check_interval(interval: Duration) -> Duration {
    if interval.is_zero() {
        DEFAULT_SERVER_HEALTH_CHECK_INTERVAL
    } else {
        interval
    }
}
