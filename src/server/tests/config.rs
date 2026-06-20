use super::*;

mod accessors;
mod constructors;
mod defaults;
mod intervals;
mod warnings;

fn recording_runtime() -> RecordingRuntime {
    RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    }
}
