use std::time::Duration;

use async_trait::async_trait;

use crate::server::LeaseBroker;
use crate::task::TaskMessage;
use crate::{LeaseError, LeaseExtension};

use super::HandlerError;

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopLeaseExtender;

#[derive(Debug, Clone, Copy, Default)]
pub struct ExtendLeaseBeforeProcess;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtendLeaseWhileProcessing {
    interval: Duration,
}

/// Extends or starts lease extension for a dequeued task before handler
/// execution.
///
/// Reference: Asynq v0.26.0 starts a lease extender goroutine around task
/// processing:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>.
#[async_trait]
pub trait LeaseExtender<B> {
    async fn before_process(
        &mut self,
        broker: &mut B,
        message: &TaskMessage,
    ) -> Result<Option<LeaseExtension>, LeaseError>;

    fn during_process_interval(&self) -> Option<Duration> {
        None
    }

    async fn during_process(
        &mut self,
        _broker: &mut B,
        _message: &TaskMessage,
    ) -> Result<Option<LeaseExtension>, LeaseError> {
        Ok(None)
    }
}

#[async_trait]
impl<B> LeaseExtender<B> for NoopLeaseExtender
where
    B: Send,
{
    async fn before_process(
        &mut self,
        _broker: &mut B,
        _message: &TaskMessage,
    ) -> Result<Option<LeaseExtension>, LeaseError> {
        Ok(None)
    }
}

#[async_trait]
impl<B> LeaseExtender<B> for ExtendLeaseBeforeProcess
where
    B: LeaseBroker + Send,
{
    async fn before_process(
        &mut self,
        broker: &mut B,
        message: &TaskMessage,
    ) -> Result<Option<LeaseExtension>, LeaseError> {
        broker
            .extend_leases(&message.queue, std::slice::from_ref(&message.id))
            .await
            .map(Some)
    }
}

impl ExtendLeaseWhileProcessing {
    pub fn every(interval: Duration) -> Self {
        Self { interval }
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

#[async_trait]
impl<B> LeaseExtender<B> for ExtendLeaseWhileProcessing
where
    B: LeaseBroker + Send,
{
    async fn before_process(
        &mut self,
        _broker: &mut B,
        _message: &TaskMessage,
    ) -> Result<Option<LeaseExtension>, LeaseError> {
        Ok(None)
    }

    fn during_process_interval(&self) -> Option<Duration> {
        if self.interval.is_zero() {
            None
        } else {
            Some(self.interval)
        }
    }

    async fn during_process(
        &mut self,
        broker: &mut B,
        message: &TaskMessage,
    ) -> Result<Option<LeaseExtension>, LeaseError> {
        broker
            .extend_leases(&message.queue, std::slice::from_ref(&message.id))
            .await
            .map(Some)
    }
}

pub fn lease_expired_error() -> HandlerError {
    // Reference: Asynq v0.26.0 exported `HandlerError::LeaseExpired` sentinel:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L80-L82>.
    HandlerError::LeaseExpired
}
