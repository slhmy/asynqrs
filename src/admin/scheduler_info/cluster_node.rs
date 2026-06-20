/// Redis cluster node serving a queue hash slot.
///
/// Reference: Asynq v0.26.0 public `ClusterNode` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L865-L877>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    id: String,
    addr: String,
}

impl ClusterNode {
    pub fn new(id: String, addr: String) -> Self {
        Self { id, addr }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns mutable access to the cluster node ID.
    ///
    /// Reference: Asynq v0.26.0 public `ClusterNode.ID` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L865-L877>.
    pub fn id_mut(&mut self) -> &mut String {
        &mut self.id
    }
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Returns mutable access to the cluster node address.
    ///
    /// Reference: Asynq v0.26.0 public `ClusterNode.Addr` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L865-L877>.
    pub fn addr_mut(&mut self) -> &mut String {
        &mut self.addr
    }
}
