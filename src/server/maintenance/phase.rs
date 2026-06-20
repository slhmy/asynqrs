#[derive(Debug, Clone, Copy)]
pub(in crate::server) enum ServerMaintenancePhase {
    Forwarder,
    Recoverer,
    Janitor,
}
