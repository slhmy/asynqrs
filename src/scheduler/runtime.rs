use tokio::sync::{mpsc, watch};

use super::SchedulerError;
use super::broker::SchedulerBroker;
use super::core::Scheduler;
use super::defaults::scheduler_heartbeat;
use super::handle::{SchedulerCommand, SchedulerHandle, SchedulerRun};
use super::logging::log_scheduler_info;
use crate::server::{Sleeper, TokioSleeper};

mod enqueue;
mod metadata;

impl<B, C> Scheduler<B, C>
where
    B: SchedulerBroker + Send,
    C: crate::client::Clock + Send + Sync,
{
    pub async fn run_once(&mut self) -> Result<SchedulerRun, SchedulerError> {
        let run = self.run_due_entries().await?;
        self.write_metadata_ignoring_error().await;
        Ok(run)
    }

    pub(crate) async fn run_until_stopped<S>(
        &mut self,
        sleeper: &mut S,
        shutdown: watch::Receiver<bool>,
    ) -> Result<SchedulerRun, SchedulerError>
    where
        S: Sleeper + Send,
    {
        self.begin_start()?;
        self.log_startup();
        let (_command_tx, command_rx) = mpsc::channel(1);
        self.run_started_until_stopped(sleeper, shutdown, command_rx)
            .await
    }

    /// Reference: Asynq v0.26.0 public `Scheduler.Run` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L285-L293>.
    pub async fn run(&mut self) -> Result<SchedulerRun, SchedulerError> {
        let mut sleeper = TokioSleeper;
        let shutdown = crate::signal::os_shutdown_receiver();
        self.run_until_stopped(&mut sleeper, shutdown).await
    }

    async fn run_started_until_stopped<S>(
        &mut self,
        sleeper: &mut S,
        mut shutdown: watch::Receiver<bool>,
        mut commands: mpsc::Receiver<SchedulerCommand>,
    ) -> Result<SchedulerRun, SchedulerError>
    where
        S: Sleeper + Send,
    {
        let mut summary = SchedulerRun::default();
        let result = async {
            let mut heartbeat = scheduler_heartbeat(self.heartbeat_interval);
            heartbeat.tick().await;
            while !*shutdown.borrow() {
                summary.merge(self.run_due_entries().await?);
                tokio::select! {
                    _ = heartbeat.tick() => {
                        // Reference: Asynq v0.26.0 scheduler runs a
                        // heartbeater goroutine which periodically writes the
                        // current cron entry snapshot to Redis:
                        // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L319-L341>.
                        self.write_metadata_ignoring_error().await;
                    }
                    _ = sleeper.sleep(self.tick_interval) => {}
                    command = commands.recv() => {
                        if let Some(command) = command {
                            self.apply_command(command).await;
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            log_scheduler_info(
                &self.logger,
                self.log_level,
                format_args!("Scheduler shutting down"),
            );
            self.clear_runtime_metadata().await?;
            log_scheduler_info(
                &self.logger,
                self.log_level,
                format_args!("Scheduler stopped"),
            );
            Ok(summary)
        }
        .await;
        self.close();
        result
    }

    async fn apply_command(&mut self, command: SchedulerCommand) {
        match command {
            SchedulerCommand::RegisterEvery {
                entry_id,
                task,
                spec,
                interval,
                options,
                result,
            } => {
                let _ = result.send(self.register_every(entry_id, task, spec, interval, options));
            }
            SchedulerCommand::Register {
                entry_id,
                task,
                spec,
                options,
                result,
            } => {
                let _ = result.send(self.register_spec_with(entry_id, task, spec, options));
            }
            SchedulerCommand::Unregister { entry_id, result } => {
                let _ = result.send(self.unregister(&entry_id));
            }
            SchedulerCommand::Ping { result } => {
                let _ = result.send(self.broker.ping().await.map_err(SchedulerError::Ping));
            }
        }
    }

    /// Starts the scheduler in a background task and returns immediately.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Start` starts scheduler processing
    /// asynchronously and returns startup state errors for already-running or
    /// already-stopped schedulers:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L295-L308>.
    pub fn start(self) -> Result<SchedulerHandle, SchedulerError>
    where
        B: 'static,
        C: 'static,
    {
        self.start_with_sleeper(TokioSleeper)
    }

    /// Starts the scheduler with an injected sleeper for deterministic Rust
    /// runtimes and tests.
    ///
    /// Reference: The public upstream-shaped `Scheduler.Start` uses the
    /// scheduler's configured runtime ticker directly:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L295-L308>.
    pub(crate) fn start_with_sleeper<S>(
        mut self,
        mut sleeper: S,
    ) -> Result<SchedulerHandle, SchedulerError>
    where
        B: 'static,
        C: 'static,
        S: Sleeper + Send + 'static,
    {
        self.begin_start()?;
        self.log_startup();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (command_tx, command_rx) = mpsc::channel(16);
        let run = tokio::spawn(async move {
            self.run_started_until_stopped(&mut sleeper, shutdown_rx, command_rx)
                .await
        });
        Ok(SchedulerHandle {
            shutdown: shutdown_tx,
            commands: command_tx,
            run,
        })
    }

    fn log_startup(&self) {
        // Reference: Asynq v0.26.0 `Scheduler.Start` logs successful startup
        // and the configured scheduler timezone before starting cron:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L295-L303>.
        log_scheduler_info(
            &self.logger,
            self.log_level,
            format_args!("Scheduler starting"),
        );
        log_scheduler_info(
            &self.logger,
            self.log_level,
            format_args!("Scheduler timezone is set to {}", self.timezone),
        );
    }
}
