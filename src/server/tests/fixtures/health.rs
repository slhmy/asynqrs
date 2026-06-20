use super::*;

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct RecordingHealthCheckHandler {
    pub(in crate::server::tests) results: Arc<StdMutex<Vec<Result<(), String>>>>,
}

impl HealthCheckHandler for RecordingHealthCheckHandler {
    fn handle(&self, result: Result<(), String>) {
        self.results
            .lock()
            .expect("health check results poisoned")
            .push(result);
    }
}
