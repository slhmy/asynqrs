use super::*;

#[derive(Debug, Default)]
pub(crate) struct RecordingAggregationHandler {
    pub(crate) handled: Vec<(String, String, String, usize)>,
    pub(crate) task_type: Option<String>,
    pub(crate) failures_before_success: usize,
}

#[async_trait]
impl AggregationHandler for RecordingAggregationHandler {
    async fn handle_aggregation(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
        set: AggregationSet,
    ) -> Result<Task, AggregationError> {
        let task_type = self
            .task_type
            .clone()
            .unwrap_or_else(|| "email:batch".to_owned());
        self.handled.push((
            queue.to_owned(),
            group.to_owned(),
            set_id.to_owned(),
            set.messages.len(),
        ));
        if self.failures_before_success > 0 {
            self.failures_before_success -= 1;
            return Err(AggregationError::Handler("combine failed".to_owned()));
        }
        Ok(Task::new(task_type, b"aggregated".to_vec()))
    }
}
