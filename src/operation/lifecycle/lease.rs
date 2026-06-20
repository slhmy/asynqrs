use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseExtension {
    expires_at: SystemTime,
}

impl LeaseExtension {
    pub fn new(expires_at: SystemTime) -> Self {
        Self { expires_at }
    }

    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }
}
