/// Summary of lease-expired active tasks recovered in one batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoverResult {
    retried: usize,
    archived: usize,
}

impl RecoverResult {
    pub fn new(retried: usize, archived: usize) -> Self {
        Self { retried, archived }
    }

    pub fn retried(&self) -> usize {
        self.retried
    }

    pub fn archived(&self) -> usize {
        self.archived
    }

    pub fn total(&self) -> usize {
        self.retried + self.archived
    }
}
