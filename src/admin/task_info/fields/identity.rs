use crate::TaskState;
use crate::task::TaskMessage;

use super::super::TaskInfo;

impl TaskInfo {
    pub(crate) fn message(&self) -> &TaskMessage {
        &self.message
    }

    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Returns mutable access to the task-info state.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.State` is an exported field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L83-L120>.
    pub fn state_mut(&mut self) -> &mut TaskState {
        &mut self.state
    }
    pub fn id(&self) -> &str {
        &self.message.id
    }

    /// Returns mutable access to the task-info ID.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.ID` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L83-L95>.
    pub fn id_mut(&mut self) -> &mut String {
        &mut self.message.id
    }
    pub fn queue(&self) -> &str {
        &self.message.queue
    }

    /// Returns mutable access to the task-info queue name.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Queue` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L83-L95>.
    pub fn queue_mut(&mut self) -> &mut String {
        &mut self.message.queue
    }
    /// Returns the task type name.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Type` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L94-L95>.
    pub fn task_type(&self) -> &str {
        &self.message.r#type
    }

    /// Returns mutable access to the task-info type name.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Type` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L83-L95>.
    pub fn task_type_mut(&mut self) -> &mut String {
        &mut self.message.r#type
    }
    pub fn type_name(&self) -> &str {
        self.task_type()
    }

    /// Returns the error message from the task's last failure.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.LastErr` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L109-L110>.
    pub fn last_err(&self) -> &str {
        &self.message.error_msg
    }

    /// Returns mutable access to the task-info last-error text.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.LastErr` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L100-L112>.
    pub fn last_err_mut(&mut self) -> &mut String {
        &mut self.message.error_msg
    }

    pub fn last_error(&self) -> &str {
        self.last_err()
    }
    pub fn group(&self) -> &str {
        &self.message.group_key
    }

    /// Returns mutable access to the task-info group name.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Group` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L108-L109>.
    pub fn group_mut(&mut self) -> &mut String {
        &mut self.message.group_key
    }
}
