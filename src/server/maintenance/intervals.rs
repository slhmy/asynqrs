use std::time::Duration;

use super::tickers::maintenance_ticker;
use crate::server::{
    DEFAULT_SERVER_FORWARDER_INTERVAL, DEFAULT_SERVER_JANITOR_INTERVAL,
    DEFAULT_SERVER_RECOVERER_INTERVAL,
};

#[derive(Debug, Clone, Copy)]
pub(in crate::server) struct ServerMaintenanceIntervals {
    pub(in crate::server) forwarder: Duration,
    pub(in crate::server) recoverer: Duration,
    pub(in crate::server) janitor: Duration,
}

impl ServerMaintenanceIntervals {
    /// Reference: Asynq v0.26.0 starts forwarder, recoverer, and janitor as
    /// separate lifecycle components with independently configured intervals:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L529-L594>.
    pub(in crate::server) fn new(
        forwarder: Duration,
        recoverer: Duration,
        janitor: Duration,
    ) -> Self {
        Self {
            forwarder: normalize_interval(forwarder, DEFAULT_SERVER_FORWARDER_INTERVAL),
            recoverer: normalize_interval(recoverer, DEFAULT_SERVER_RECOVERER_INTERVAL),
            janitor: normalize_interval(janitor, DEFAULT_SERVER_JANITOR_INTERVAL),
        }
    }

    pub(in crate::server) fn forwarder_ticker(self) -> tokio::time::Interval {
        maintenance_ticker(self.forwarder)
    }

    pub(in crate::server) fn recoverer_ticker(self) -> tokio::time::Interval {
        maintenance_ticker(self.recoverer)
    }

    pub(in crate::server) fn janitor_ticker(self) -> tokio::time::Interval {
        maintenance_ticker(self.janitor)
    }
}

pub(in crate::server) fn normalize_interval(duration: Duration, default: Duration) -> Duration {
    if duration.is_zero() {
        default
    } else {
        duration
    }
}
