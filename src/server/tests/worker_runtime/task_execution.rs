use super::*;

mod lease;
mod retry;
mod success;
mod sync;
mod terminal;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedHookTask {
    has_result_writer: bool,
    deadline: Option<SystemTime>,
    is_cancelled: bool,
    task_id: Option<String>,
    queue_name: Option<String>,
    retry_count: Option<i32>,
    max_retry: Option<i32>,
    trace_id: Option<String>,
}

impl ObservedHookTask {
    fn from_task(task: &Task, context: &ProcessingContext) -> Self {
        Self {
            has_result_writer: context.result_writer().is_some(),
            deadline: context.deadline(),
            is_cancelled: context.is_cancelled(),
            task_id: Some(context.task_id().to_owned()),
            queue_name: Some(context.queue_name().to_owned()),
            retry_count: Some(context.retry_count()),
            max_retry: Some(context.max_retry()),
            trace_id: task.header("trace-id").map(str::to_owned),
        }
    }

    fn from_task_without_context(task: &Task) -> Self {
        Self {
            has_result_writer: false,
            deadline: None,
            is_cancelled: false,
            task_id: None,
            queue_name: None,
            retry_count: None,
            max_retry: None,
            trace_id: task.header("trace-id").map(str::to_owned),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RecordingErrorHandler {
    tasks: Arc<std::sync::Mutex<Vec<ObservedHookTask>>>,
}

#[async_trait]
impl ErrorHandler for RecordingErrorHandler {
    async fn handle_error(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        _error: &HandlerError,
    ) {
        self.tasks
            .lock()
            .expect("observed error-handler tasks poisoned")
            .push(ObservedHookTask::from_task(task, context));
    }
}

#[derive(Debug, Clone)]
struct ObservingRetryDelay {
    tasks: Arc<std::sync::Mutex<Vec<ObservedHookTask>>>,
    delay: Duration,
}

impl ObservingRetryDelay {
    fn new(delay: Duration) -> Self {
        Self {
            tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            delay,
        }
    }
}

impl RetryDelay for ObservingRetryDelay {
    fn retry_delay(&mut self, _retried: i32, _error: &HandlerError, task: &Task) -> Duration {
        self.tasks
            .lock()
            .expect("observed retry-delay tasks poisoned")
            .push(ObservedHookTask::from_task_without_context(task));
        self.delay
    }
}
