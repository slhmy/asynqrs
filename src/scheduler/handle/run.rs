#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SchedulerRun {
    pub(in crate::scheduler) enqueued: usize,
}

impl SchedulerRun {
    pub fn enqueued(&self) -> usize {
        self.enqueued
    }

    pub(in crate::scheduler) fn merge(&mut self, other: Self) {
        self.enqueued += other.enqueued;
    }
}
