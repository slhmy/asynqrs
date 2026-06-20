use std::sync::Arc;

use super::Scheduler;
use crate::{LogLevel, Logger};

impl<B, C> Scheduler<B, C> {
    pub fn log_level(&self) -> LogLevel {
        self.log_level
    }

    pub fn logger(&self) -> Option<&Arc<dyn Logger>> {
        self.logger.as_ref()
    }
}
