use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::config::{PeriodicTaskConfig, PeriodicTaskConfigKey};
use super::{PeriodicTaskConfigProvider, PeriodicTaskConfigProviderError};
use crate::client::Clock;
use crate::scheduler::{SchedulerBroker, SchedulerConstructionError};
use crate::server::Sleeper;
use crate::{LogLevel, Logger, Scheduler, SchedulerError, SchedulerHandle};

/// Default interval between periodic task config syncs.
///
/// Reference: Asynq v0.26.0 `DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go#L43-L44>.
pub const DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL: Duration = Duration::from_secs(180);

/// Dynamically syncs periodic task configs into a scheduler.
///
/// Reference: Asynq v0.26.0 `PeriodicTaskManager` registers configs returned
/// by its provider and unregisters configs that disappear on later syncs:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
///
#[derive(Debug)]
pub struct PeriodicTaskManager<P, B, C = crate::SystemClock> {
    provider: P,
    scheduler: Scheduler<B, C>,
    entry_ids: HashMap<PeriodicTaskConfigKey, String>,
    sync_interval: Duration,
}

impl<P, B, C> PeriodicTaskManager<P, B, C> {
    pub fn new(provider: P, scheduler: Scheduler<B, C>) -> Self {
        Self {
            provider,
            scheduler,
            entry_ids: HashMap::new(),
            sync_interval: DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL,
        }
    }

    pub fn with_sync_interval(mut self, sync_interval: Duration) -> Self {
        // Reference: Asynq v0.26.0 `NewPeriodicTaskManager` treats a zero
        // `SyncInterval` as the default sync interval:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go#L57-L60>.
        self.sync_interval = if sync_interval.is_zero() {
            DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL
        } else {
            sync_interval
        };
        self
    }

    pub fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    pub fn scheduler(&self) -> &Scheduler<B, C> {
        &self.scheduler
    }

    pub fn scheduler_mut(&mut self) -> &mut Scheduler<B, C> {
        &mut self.scheduler
    }

    pub fn provider(&self) -> &P {
        &self.provider
    }

    pub fn provider_mut(&mut self) -> &mut P {
        &mut self.provider
    }

    pub fn into_parts(self) -> (P, Scheduler<B, C>) {
        (self.provider, self.scheduler)
    }
}

impl<P, B, C> PeriodicTaskManager<P, B, C>
where
    P: PeriodicTaskConfigProvider,
    C: Clock,
{
    /// Syncs provider configs into scheduler registrations once.
    ///
    /// Reference: Asynq v0.26.0 periodic task manager sync adds newly returned
    /// configs and removes scheduler entries for configs no longer returned by
    /// the provider.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    pub fn sync_once(&mut self) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError> {
        Self::sync_scheduler_once(&mut self.provider, &mut self.scheduler, &mut self.entry_ids)
    }

    fn sync_scheduler_once(
        provider: &mut P,
        scheduler: &mut Scheduler<B, C>,
        entry_ids: &mut HashMap<PeriodicTaskConfigKey, String>,
    ) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError> {
        let configs = provider.get_configs()?;
        validate_periodic_task_configs(&configs)?;
        let logger = scheduler.logger().cloned();
        let log_level = scheduler.log_level();
        let diff = diff_periodic_task_configs(configs, entry_ids);
        let mut run = PeriodicTaskManagerRun {
            unchanged: diff.unchanged,
            ..Default::default()
        };

        // Reference: Asynq v0.26.0 periodic sync unregisters removed configs
        // before registering added configs:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
        for key in diff.stale {
            let Some(entry_id) = entry_ids.get(&key).cloned() else {
                continue;
            };
            match scheduler.unregister(&entry_id) {
                Ok(()) => {
                    entry_ids.remove(&key);
                    log_periodic_info(
                        &logger,
                        log_level,
                        format_args!("Successfully unregistered periodic task: entryID={entry_id}"),
                    );
                    run.unregistered += 1;
                }
                Err(error) => {
                    // Reference: Asynq v0.26.0 keeps the local periodic task
                    // hash when unregister fails so a later sync can retry:
                    // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
                    log_periodic_error(
                        &logger,
                        log_level,
                        format_args!("Failed to unregister periodic task: {error}"),
                    );
                }
            }
        }

        for config in diff.added {
            let key = config.key();
            match scheduler.register_spec_with_generated_id_and(
                config.task().clone(),
                config.cronspec(),
                config.options().clone(),
            ) {
                Ok(entry_id) => {
                    log_periodic_info(
                        &logger,
                        log_level,
                        format_args!(
                            "Successfully registered periodic task: cronspec={:?} task={:?}, entryID={entry_id}",
                            config.cronspec(),
                            config.task().type_name()
                        ),
                    );
                    entry_ids.insert(key, entry_id);
                    run.registered += 1;
                }
                Err(error) => {
                    // Reference: Asynq v0.26.0 logs per-config registration
                    // failures and continues syncing the remaining periodic
                    // task configs:
                    // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
                    log_periodic_error(
                        &logger,
                        log_level,
                        format_args!(
                            "Failed to register periodic task: cronspec={:?} task={:?} err={error}",
                            config.cronspec(),
                            config.task().type_name()
                        ),
                    );
                }
            }
        }

        Ok(run)
    }

    fn initial_sync_scheduler_once(
        provider: &mut P,
        scheduler: &mut Scheduler<B, C>,
        entry_ids: &mut HashMap<PeriodicTaskConfigKey, String>,
    ) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError> {
        // Reference: Asynq v0.26.0 `PeriodicTaskManager.Start` wraps initial
        // provider and validation failures before starting the scheduler:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
        Self::sync_scheduler_once(provider, scheduler, entry_ids).map_err(startup_sync_error)
    }
}

impl<P, B, C> PeriodicTaskManager<P, B, C>
where
    P: PeriodicTaskConfigProvider + Send + 'static,
    B: SchedulerBroker + Send + 'static,
    C: Clock + Send + Sync + 'static,
{
    /// Runs the manager until shutdown, syncing provider configs into a
    /// background scheduler at the configured interval.
    ///
    /// Reference: Asynq v0.26.0 `PeriodicTaskManager.Run` starts the
    /// underlying scheduler and periodically syncs configs from its provider:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    pub(crate) async fn run_until_stopped<SS, MS>(
        self,
        scheduler_sleeper: SS,
        sync_sleeper: &mut MS,
        shutdown: watch::Receiver<bool>,
    ) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError>
    where
        SS: Sleeper + Send + 'static,
        MS: Sleeper + Send,
    {
        let Self {
            mut provider,
            mut scheduler,
            mut entry_ids,
            sync_interval,
        } = self;
        // Reference: Asynq v0.26.0 `PeriodicTaskManager.Start` performs an
        // initial sync before starting its embedded scheduler and returns the
        // sync error to the caller:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
        let mut summary = PeriodicTaskManagerRun::default();
        summary.record_sync(Self::initial_sync_scheduler_once(
            &mut provider,
            &mut scheduler,
            &mut entry_ids,
        )?);
        let logger = scheduler.logger().cloned();
        let log_level = scheduler.log_level();
        let handle = scheduler
            .start_with_sleeper(scheduler_sleeper)
            .map_err(startup_scheduler_error)?;
        summary.merge(
            PeriodicSyncRuntime {
                provider,
                handle,
                entry_ids,
                sync_interval,
                sync_sleeper,
                shutdown,
                logger,
                log_level,
            }
            .run()
            .await?,
        );
        Ok(summary)
    }

    /// Reference: Asynq v0.26.0 public `PeriodicTaskManager.Run` method
    /// waits for an OS signal before shutting down the manager:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    pub async fn run(self) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError> {
        let logger = self.scheduler.logger().cloned();
        let log_level = self.scheduler.log_level();
        let shutdown = crate::signal::os_shutdown_receiver();
        let mut sync_sleeper = crate::server::TokioSleeper;
        let result = self
            .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown)
            .await;
        if result.is_ok() {
            // Reference: Asynq v0.26.0 `PeriodicTaskManager.Run` logs a
            // debug exit message after signal-driven shutdown completes:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
            log_periodic_debug(&logger, log_level, "PeriodicTaskManager exiting");
        }
        result
    }

    /// Starts the manager in a background task and returns immediately.
    ///
    /// Reference: Asynq v0.26.0 public `PeriodicTaskManager.Start` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    pub fn start(self) -> Result<PeriodicTaskManagerHandle, PeriodicTaskManagerError> {
        self.start_with_sleepers(crate::server::TokioSleeper, crate::server::TokioSleeper)
    }

    /// Starts the manager with injected scheduler and sync sleepers for
    /// deterministic Rust runtimes and tests.
    ///
    /// Reference: The public upstream-shaped `PeriodicTaskManager.Start`
    /// starts its internal scheduler and sync loop without caller-supplied
    /// sleeper arguments:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    pub(crate) fn start_with_sleepers<SS, MS>(
        self,
        scheduler_sleeper: SS,
        mut sync_sleeper: MS,
    ) -> Result<PeriodicTaskManagerHandle, PeriodicTaskManagerError>
    where
        SS: Sleeper + Send + 'static,
        MS: Sleeper + Send + 'static,
    {
        let Self {
            mut provider,
            mut scheduler,
            mut entry_ids,
            sync_interval,
        } = self;
        // Reference: Asynq v0.26.0 `PeriodicTaskManager.Start` performs an
        // initial sync before starting cron scheduling:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
        let initial_sync =
            Self::initial_sync_scheduler_once(&mut provider, &mut scheduler, &mut entry_ids)?;
        let logger = scheduler.logger().cloned();
        let log_level = scheduler.log_level();
        let scheduler_handle = scheduler
            .start_with_sleeper(scheduler_sleeper)
            .map_err(startup_scheduler_error)?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run = tokio::spawn(async move {
            let mut summary = PeriodicTaskManagerRun::default();
            summary.record_sync(initial_sync);
            summary.merge(
                PeriodicSyncRuntime {
                    provider,
                    handle: scheduler_handle,
                    entry_ids,
                    sync_interval,
                    sync_sleeper: &mut sync_sleeper,
                    shutdown: shutdown_rx,
                    logger,
                    log_level,
                }
                .run()
                .await?,
            );
            Ok(summary)
        });
        Ok(PeriodicTaskManagerHandle {
            shutdown: shutdown_tx,
            run,
        })
    }
}

struct PeriodicSyncRuntime<'a, P, MS> {
    provider: P,
    handle: SchedulerHandle,
    entry_ids: HashMap<PeriodicTaskConfigKey, String>,
    sync_interval: Duration,
    sync_sleeper: &'a mut MS,
    shutdown: watch::Receiver<bool>,
    logger: Option<Arc<dyn Logger>>,
    log_level: LogLevel,
}

impl<P, MS> PeriodicSyncRuntime<'_, P, MS>
where
    P: PeriodicTaskConfigProvider,
    MS: Sleeper + Send,
{
    async fn run(mut self) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError> {
        let mut summary = PeriodicTaskManagerRun::default();
        while !*self.shutdown.borrow() {
            tokio::select! {
                _ = self.sync_sleeper.sleep(self.sync_interval) => {}
                changed = self.shutdown.changed() => {
                    if changed.is_err() || *self.shutdown.borrow() {
                        break;
                    }
                    continue;
                }
            }
            if *self.shutdown.borrow() {
                break;
            }
            match sync_handle_once(
                &mut self.provider,
                &self.handle,
                &mut self.entry_ids,
                &self.logger,
                self.log_level,
            )
            .await
            {
                Ok(run) => summary.record_sync(run),
                Err(error) => {
                    // Reference: Asynq v0.26.0 logs periodic sync errors and
                    // keeps the manager and embedded scheduler running for later
                    // sync attempts:
                    // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
                    log_runtime_sync_error(&self.logger, self.log_level, &error);
                }
            }
        }

        // Reference: Asynq v0.26.0 `PeriodicTaskManager.Start` logs when the
        // background syncer goroutine receives its shutdown signal:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
        log_periodic_debug(&self.logger, self.log_level, "Stopping syncer goroutine");
        let scheduler_run = self.handle.shutdown().await?;
        summary.scheduler_enqueued += scheduler_run.enqueued();
        Ok(summary)
    }
}

/// Handle returned by `PeriodicTaskManager::start`.
///
/// Reference: Asynq v0.26.0 `PeriodicTaskManager.Start` starts background
/// sync processing, while `Shutdown` later stops manager and scheduler work:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
#[derive(Debug)]
pub struct PeriodicTaskManagerHandle {
    shutdown: watch::Sender<bool>,
    run: JoinHandle<Result<PeriodicTaskManagerRun, PeriodicTaskManagerError>>,
}

impl PeriodicTaskManagerHandle {
    /// Stops the running periodic task manager and waits for shutdown.
    ///
    /// Reference: Asynq v0.26.0 public `PeriodicTaskManager.Shutdown` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    pub async fn shutdown(self) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError> {
        let _ = self.shutdown.send(true);
        self.wait().await
    }

    /// Waits until a separately stopped manager exits or fails.
    pub async fn wait(self) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError> {
        self.run.await.unwrap_or_else(|error| {
            Err(PeriodicTaskManagerError::BackgroundTask(error.to_string()))
        })
    }
}

async fn sync_handle_once<P>(
    provider: &mut P,
    handle: &SchedulerHandle,
    entry_ids: &mut HashMap<PeriodicTaskConfigKey, String>,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> Result<PeriodicTaskManagerRun, PeriodicTaskManagerError>
where
    P: PeriodicTaskConfigProvider,
{
    let configs = provider.get_configs()?;
    validate_periodic_task_configs(&configs)?;
    let diff = diff_periodic_task_configs(configs, entry_ids);
    let mut run = PeriodicTaskManagerRun {
        unchanged: diff.unchanged,
        ..Default::default()
    };

    // Reference: Asynq v0.26.0 periodic sync unregisters removed configs
    // before registering added configs:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    for key in diff.stale {
        let Some(entry_id) = entry_ids.get(&key).cloned() else {
            continue;
        };
        match handle.unregister(&entry_id).await {
            Ok(()) => {
                entry_ids.remove(&key);
                log_periodic_info(
                    logger,
                    log_level,
                    format_args!("Successfully unregistered periodic task: entryID={entry_id}"),
                );
                run.unregistered += 1;
            }
            Err(error) => {
                // Reference: Asynq v0.26.0 only removes a periodic task hash
                // after unregister succeeds:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
                log_periodic_error(
                    logger,
                    log_level,
                    format_args!("Failed to unregister periodic task: {error}"),
                );
            }
        }
    }

    for config in diff.added {
        let key = config.key();
        match handle
            .register_spec_with_generated_id_and(
                config.task().clone(),
                config.cronspec(),
                config.options().clone(),
            )
            .await
        {
            Ok(entry_id) => {
                log_periodic_info(
                    logger,
                    log_level,
                    format_args!(
                        "Successfully registered periodic task: cronspec={:?} task={:?}, entryID={entry_id}",
                        config.cronspec(),
                        config.task().type_name()
                    ),
                );
                entry_ids.insert(key, entry_id);
                run.registered += 1;
            }
            Err(error) => {
                // Reference: Asynq v0.26.0 logs per-config registration
                // failures and keeps the syncer running:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
                log_periodic_error(
                    logger,
                    log_level,
                    format_args!(
                        "Failed to register periodic task: cronspec={:?} task={:?} err={error}",
                        config.cronspec(),
                        config.task().type_name()
                    ),
                );
            }
        }
    }

    Ok(run)
}

struct PeriodicTaskConfigDiff {
    added: Vec<PeriodicTaskConfig>,
    stale: Vec<PeriodicTaskConfigKey>,
    unchanged: usize,
}

fn diff_periodic_task_configs(
    configs: Vec<PeriodicTaskConfig>,
    entry_ids: &HashMap<PeriodicTaskConfigKey, String>,
) -> PeriodicTaskConfigDiff {
    let desired = configs
        .iter()
        .map(PeriodicTaskConfig::key)
        .collect::<HashSet<_>>();
    let added = configs
        .into_iter()
        .filter(|config| !entry_ids.contains_key(&config.key()))
        .collect::<Vec<_>>();
    let stale = entry_ids
        .keys()
        .filter(|key| !desired.contains(*key))
        .cloned()
        .collect::<Vec<_>>();
    let unchanged = desired
        .iter()
        .filter(|key| entry_ids.contains_key(*key))
        .count();

    PeriodicTaskConfigDiff {
        added,
        stale,
        unchanged,
    }
}

fn validate_periodic_task_configs(
    configs: &[PeriodicTaskConfig],
) -> Result<(), PeriodicTaskManagerError> {
    for config in configs {
        // Reference: Asynq v0.26.0 validates provider configs before
        // diffing/registering periodic tasks and treats an empty Cronspec as
        // an invalid provider config:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
        //
        // Rust compatibility: the provider boundary accepts concrete configs,
        // so upstream nil config/task validation is represented by type safety.
        if config.cronspec().is_empty() {
            return Err(PeriodicTaskManagerError::InvalidConfig(
                "PeriodicTaskConfig.Cronspec cannot be empty".to_owned(),
            ));
        }
    }
    Ok(())
}

fn log_periodic_info(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    if matches!(log_level, LogLevel::Debug | LogLevel::Info) {
        if let Some(logger) = logger {
            logger.info(args);
        }
    }
}

fn log_periodic_debug(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    message: &'static str,
) {
    if matches!(log_level, LogLevel::Debug) {
        if let Some(logger) = logger {
            logger.debug(format_args!("{message}"));
        }
    }
}

fn log_periodic_error(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    if matches!(
        log_level,
        LogLevel::Debug | LogLevel::Info | LogLevel::Warn | LogLevel::Error
    ) {
        if let Some(logger) = logger {
            logger.error(args);
        }
    }
}

fn log_runtime_sync_error(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    error: &PeriodicTaskManagerError,
) {
    match error {
        PeriodicTaskManagerError::InvalidConfig(error) => log_periodic_error(
            logger,
            log_level,
            format_args!("Failed to sync: GetConfigs returned an invalid config: {error}"),
        ),
        _ => log_periodic_error(
            logger,
            log_level,
            format_args!("Failed to get periodic task configs: {error}"),
        ),
    }
}

fn startup_sync_error(error: PeriodicTaskManagerError) -> PeriodicTaskManagerError {
    match error {
        PeriodicTaskManagerError::Provider(error) => {
            PeriodicTaskManagerError::Startup(format!("initial call to GetConfigs failed: {error}"))
        }
        PeriodicTaskManagerError::InvalidConfig(error) => PeriodicTaskManagerError::Startup(
            format!("initial call to GetConfigs contained an invalid config: {error}"),
        ),
        error => error,
    }
}

fn startup_scheduler_error(error: SchedulerError) -> PeriodicTaskManagerError {
    // Reference: Asynq v0.26.0 `PeriodicTaskManager.Start` wraps embedded
    // scheduler startup failures with an outer `asynq:` error:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
    PeriodicTaskManagerError::Startup(error.to_string())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeriodicTaskManagerRun {
    registered: usize,
    unregistered: usize,
    unchanged: usize,
    syncs: usize,
    scheduler_enqueued: usize,
}

impl PeriodicTaskManagerRun {
    pub fn registered(&self) -> usize {
        self.registered
    }

    pub fn unregistered(&self) -> usize {
        self.unregistered
    }

    pub fn unchanged(&self) -> usize {
        self.unchanged
    }

    pub fn syncs(&self) -> usize {
        self.syncs
    }

    pub fn scheduler_enqueued(&self) -> usize {
        self.scheduler_enqueued
    }

    fn record_sync(&mut self, other: Self) {
        self.registered += other.registered;
        self.unregistered += other.unregistered;
        self.unchanged += other.unchanged;
        self.scheduler_enqueued += other.scheduler_enqueued;
        self.syncs += 1;
    }

    fn merge(&mut self, other: Self) {
        self.registered += other.registered;
        self.unregistered += other.unregistered;
        self.unchanged += other.unchanged;
        self.syncs += other.syncs;
        self.scheduler_enqueued += other.scheduler_enqueued;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PeriodicTaskManagerError {
    #[error("{0}")]
    Provider(#[from] PeriodicTaskConfigProviderError),
    #[error("{0}")]
    Scheduler(#[from] SchedulerError),
    #[error("{0}")]
    SchedulerConstruction(#[from] SchedulerConstructionError),
    #[error("{0}")]
    BackgroundTask(String),
    #[error("asynq: {0}")]
    Startup(String),
    #[error("{0}")]
    InvalidConfig(String),
}
