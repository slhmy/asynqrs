use std::collections::HashMap;

use bytes::Bytes;

use super::super::TaskInfo;

impl TaskInfo {
    pub fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    pub fn payload_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.message.payload)
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.message.payload
    }

    /// Returns mutable access to the task-info payload.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Payload` is an exported
    /// byte-slice field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L96-L99>.
    pub fn payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.message.payload
    }
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.message.headers
    }

    /// Returns mutable access to the task-info headers.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Headers` is an exported map
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L98-L99>.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.message.headers
    }
    pub fn result(&self) -> &[u8] {
        &self.result
    }

    pub fn result_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.result)
    }

    pub fn into_result(self) -> Vec<u8> {
        self.result
    }

    /// Returns mutable access to the task-info result bytes.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Result` is an exported
    /// byte-slice field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L119-L120>.
    pub fn result_mut(&mut self) -> &mut Vec<u8> {
        &mut self.result
    }
}
