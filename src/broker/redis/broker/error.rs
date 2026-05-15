use thiserror::Error;

use crate::{
    ArchiveError, BrokerError, CompleteError, DecodeTaskMessageError, DequeueError, ForwardError,
    LeaseError, RecoverError, RedisArchivePlanError, RedisCompletePlanError, RedisDequeuePlanError,
    RedisEnqueuePlanError, RedisExtendLeasePlanError, RedisForwardPlanError, RedisRecoverPlanError,
    RedisRequeuePlanError, RedisRetryPlanError, RedisScript, RedisScriptCallError, RequeueError,
    RetryError,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct RedisExecutorError {
    message: String,
}

#[derive(Debug, Error)]
pub enum RedisBrokerError {
    #[error("failed to build Redis enqueue plan: {0}")]
    Plan(#[from] RedisEnqueuePlanError),
    #[error("failed to build Redis dequeue plan: {0}")]
    DequeuePlan(#[from] RedisDequeuePlanError),
    #[error("failed to build Redis complete plan: {0}")]
    CompletePlan(#[from] RedisCompletePlanError),
    #[error("failed to build Redis retry plan: {0}")]
    RetryPlan(#[from] RedisRetryPlanError),
    #[error("failed to build Redis archive plan: {0}")]
    ArchivePlan(#[from] RedisArchivePlanError),
    #[error("failed to build Redis requeue plan: {0}")]
    RequeuePlan(#[from] RedisRequeuePlanError),
    #[error("failed to build Redis forward plan: {0}")]
    ForwardPlan(#[from] RedisForwardPlanError),
    #[error("failed to build Redis recover plan: {0}")]
    RecoverPlan(#[from] RedisRecoverPlanError),
    #[error("failed to build Redis extend lease plan: {0}")]
    ExtendLeasePlan(#[from] RedisExtendLeasePlanError),
    #[error("invalid Redis script call: {0}")]
    ScriptCall(#[from] RedisScriptCallError),
    #[error("Redis executor failed: {0}")]
    Executor(#[from] RedisExecutorError),
    #[error("failed to decode dequeued task message: {0}")]
    Decode(#[from] DecodeTaskMessageError),
    #[error("unexpected {script:?} script result: {result}")]
    UnexpectedScriptResult { script: RedisScript, result: i64 },
    #[error("unexpected {script:?} script status: {status}")]
    UnexpectedScriptStatus { script: RedisScript, status: String },
}

impl RedisExecutorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<RedisBrokerError> for RecoverError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for BrokerError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for DequeueError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for CompleteError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for RetryError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for ArchiveError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for RequeueError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for ForwardError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for LeaseError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}
