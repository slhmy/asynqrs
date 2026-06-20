use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::client::Clock;

#[derive(Debug, Clone, Copy)]
pub(in crate::broker::redis::broker::tests) struct TestClock(
    pub(in crate::broker::redis::broker::tests) SystemTime,
);

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug, Clone)]
pub(in crate::broker::redis::broker::tests) struct RecordingClock {
    pub(in crate::broker::redis::broker::tests) now: SystemTime,
    pub(in crate::broker::redis::broker::tests) call_log: Arc<Mutex<Vec<&'static str>>>,
}

impl Clock for RecordingClock {
    fn now(&self) -> SystemTime {
        self.call_log.lock().unwrap().push("clock");
        self.now
    }
}

#[derive(Debug, Clone)]
pub(in crate::broker::redis::broker::tests) struct SequenceRecordingClock {
    pub(in crate::broker::redis::broker::tests) times: Arc<Mutex<Vec<SystemTime>>>,
    pub(in crate::broker::redis::broker::tests) call_log: Arc<Mutex<Vec<&'static str>>>,
}

impl Clock for SequenceRecordingClock {
    fn now(&self) -> SystemTime {
        self.call_log.lock().unwrap().push("clock");
        self.times
            .lock()
            .unwrap()
            .pop()
            .expect("expected recorded clock time")
    }
}
