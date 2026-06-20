use std::time::Duration;

use super::{ServerMaintenanceIntervals, ServerMaintenancePhase};

pub(in crate::server) struct ServerMaintenanceTickers {
    forwarder: tokio::time::Interval,
    recoverer: tokio::time::Interval,
    janitor: tokio::time::Interval,
}

impl ServerMaintenanceTickers {
    pub(in crate::server) fn new(intervals: ServerMaintenanceIntervals) -> Self {
        Self {
            forwarder: intervals.forwarder_ticker(),
            recoverer: intervals.recoverer_ticker(),
            janitor: intervals.janitor_ticker(),
        }
    }

    pub(in crate::server) async fn skip_initial_ticks(&mut self) {
        self.forwarder.tick().await;
        self.recoverer.tick().await;
        self.janitor.tick().await;
    }

    pub(in crate::server) async fn next_phase(&mut self) -> ServerMaintenancePhase {
        tokio::select! {
            _ = self.forwarder.tick() => ServerMaintenancePhase::Forwarder,
            _ = self.recoverer.tick() => ServerMaintenancePhase::Recoverer,
            _ = self.janitor.tick() => ServerMaintenancePhase::Janitor,
        }
    }
}

pub(in crate::server) fn maintenance_ticker(duration: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}
