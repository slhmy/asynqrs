use super::*;

#[test]
fn server_with_config_warns_for_large_janitor_batch_size() {
    let logger = Arc::new(RecordingLogger::default());
    let config_logger: Arc<dyn Logger> = logger.clone();
    let config = Config {
        logger: Some(config_logger),
        janitor_batch_size: 101,
        ..Config::default()
    };

    let _server =
        Server::with_config(recording_runtime(), config, RecordingSleeper::default()).unwrap();

    let calls = logger.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "warn");
    assert_eq!(
        calls[0].1,
        "Janitor batch size of 101 is greater than the recommended batch size of 100. \
             This might cause a long-running script"
    );
}

#[test]
fn server_with_config_respects_log_level_for_large_janitor_batch_size_warning() {
    let logger = Arc::new(RecordingLogger::default());
    let config_logger: Arc<dyn Logger> = logger.clone();
    let config = Config {
        logger: Some(config_logger),
        log_level: Some(LogLevel::Error),
        janitor_batch_size: 101,
        ..Config::default()
    };

    let _server =
        Server::with_config(recording_runtime(), config, RecordingSleeper::default()).unwrap();

    assert!(logger.calls.lock().unwrap().is_empty());
}
