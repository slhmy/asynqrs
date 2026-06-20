use super::*;

pub(super) struct RecordingMiddleware {
    pub(super) name: &'static str,
    pub(super) calls: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl TaskMiddleware for RecordingMiddleware {
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        next: &mut (dyn Handler + Send),
    ) -> Result<(), HandlerError> {
        self.calls.lock().expect("calls poisoned").push(format!(
            "{}:before:{}",
            self.name,
            task.type_name()
        ));
        let result = next.process_task(task, context).await;
        self.calls.lock().expect("calls poisoned").push(format!(
            "{}:after:{}",
            self.name,
            task.type_name()
        ));
        result
    }
}
