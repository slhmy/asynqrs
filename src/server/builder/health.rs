use std::sync::Arc;

use super::super::Server;
use super::super::health::HealthCheckHandler;

impl<P, S> Server<P, S> {
    pub(in crate::server) fn with_optional_health_check_handler(
        mut self,
        handler: Option<Arc<dyn HealthCheckHandler>>,
    ) -> Self {
        self.health_check_handler = handler;
        self
    }
}
