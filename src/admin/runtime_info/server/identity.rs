use super::ServerInfo;

impl ServerInfo {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns mutable access to the server ID.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.ID` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn id_mut(&mut self) -> &mut String {
        &mut self.id
    }
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns mutable access to the server host name.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.Host` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn host_mut(&mut self) -> &mut String {
        &mut self.host
    }
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Returns mutable access to the server process ID.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.PID` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn pid_mut(&mut self) -> &mut i32 {
        &mut self.pid
    }
}
