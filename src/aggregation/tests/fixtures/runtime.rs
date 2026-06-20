use super::*;
use std::sync::Mutex as StdMutex;

#[derive(Debug)]
pub(crate) struct ShutdownAfterSleeps {
    pub(crate) sleeps: usize,
    pub(crate) release: watch::Sender<bool>,
    pub(crate) shutdown: watch::Sender<bool>,
}

#[async_trait]
impl Sleeper for ShutdownAfterSleeps {
    async fn sleep(&mut self, _duration: Duration) {
        self.sleeps += 1;
        if self.sleeps == MAX_CONCURRENT_AGGREGATION_CHECKS + 2 {
            let _ = self.release.send(true);
            let _ = self.shutdown.send(true);
        }
        tokio::task::yield_now().await;
    }
}

#[derive(Debug)]
pub(crate) struct ShutdownOnFirstSleep {
    pub(crate) shutdown: watch::Sender<bool>,
}

#[async_trait]
impl Sleeper for ShutdownOnFirstSleep {
    async fn sleep(&mut self, _duration: Duration) {
        let _ = self.shutdown.send(true);
        tokio::task::yield_now().await;
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TestClock(pub(crate) SystemTime);

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug, Default)]
pub(crate) struct RecordingLogger {
    pub(crate) logs: StdMutex<Vec<String>>,
}

impl Logger for RecordingLogger {
    fn debug(&self, args: std::fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn info(&self, args: std::fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn warn(&self, args: std::fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn error(&self, args: std::fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn fatal(&self, args: std::fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }
}
