use super::*;

#[derive(Debug, Clone, Default)]
pub(crate) struct RecordingAggregationBroker {
    pub(crate) ready_set_id: Option<String>,
    pub(crate) groups: Vec<String>,
    pub(crate) group_lists: Vec<String>,
    pub(crate) group_list_error: Option<AggregationError>,
    pub(crate) checks: Vec<(String, String, SystemTime, Duration, Duration, isize)>,
    pub(crate) check_error: Option<AggregationError>,
    pub(crate) reads: Vec<(String, String, String)>,
    pub(crate) read_error: Option<AggregationError>,
    pub(crate) aggregation_deadline: Option<SystemTime>,
    pub(crate) enqueued: Vec<EnqueuePlan>,
    pub(crate) enqueue_error: Option<BrokerError>,
    pub(crate) delete_error: Option<AggregationError>,
    pub(crate) deletes: Vec<(String, String, String)>,
    pub(crate) reclaims: Vec<(String, SystemTime)>,
    pub(crate) reclaim_error: Option<AggregationError>,
}

#[async_trait]
impl AggregationBroker for RecordingAggregationBroker {
    async fn list_aggregation_groups(
        &mut self,
        queue: &str,
    ) -> Result<Vec<String>, AggregationError> {
        self.group_lists.push(queue.to_owned());
        if let Some(error) = self.group_list_error.clone() {
            return Err(error);
        }
        Ok(self.groups.clone())
    }

    async fn aggregation_check(
        &mut self,
        queue: &str,
        group: &str,
        now: SystemTime,
        grace_period: Duration,
        max_delay: Duration,
        max_size: isize,
    ) -> Result<Option<String>, AggregationError> {
        self.checks.push((
            queue.to_owned(),
            group.to_owned(),
            now,
            grace_period,
            max_delay,
            max_size,
        ));
        if let Some(error) = self.check_error.clone() {
            return Err(error);
        }
        Ok(self.ready_set_id.clone())
    }

    async fn read_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<AggregationSet, AggregationError> {
        self.reads
            .push((queue.to_owned(), group.to_owned(), set_id.to_owned()));
        if let Some(error) = self.read_error.clone() {
            return Err(error);
        }
        Ok(AggregationSet::new(
            vec![TaskMessage::from_task(&Task::new(
                "email:welcome",
                b"payload".to_vec(),
            ))],
            self.aggregation_deadline
                .unwrap_or(SystemTime::UNIX_EPOCH + Duration::from_secs(4_000_000_000)),
        ))
    }

    async fn delete_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<(), AggregationError> {
        self.deletes
            .push((queue.to_owned(), group.to_owned(), set_id.to_owned()));
        if let Some(error) = self.delete_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn enqueue_aggregated(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        if let Some(error) = self.enqueue_error.clone() {
            return Err(error);
        }
        self.enqueued.push(plan.clone());
        Ok(())
    }

    async fn reclaim_stale_aggregation_sets(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<(), AggregationError> {
        self.reclaims.push((queue.to_owned(), now));
        if let Some(error) = self.reclaim_error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BlockingAggregationBroker {
    pub(crate) checks_started: Arc<AtomicUsize>,
    pub(crate) release: watch::Receiver<bool>,
}

#[async_trait]
impl AggregationBroker for BlockingAggregationBroker {
    async fn list_aggregation_groups(
        &mut self,
        _queue: &str,
    ) -> Result<Vec<String>, AggregationError> {
        Ok(Vec::new())
    }

    async fn aggregation_check(
        &mut self,
        _queue: &str,
        _group: &str,
        _now: SystemTime,
        _grace_period: Duration,
        _max_delay: Duration,
        _max_size: isize,
    ) -> Result<Option<String>, AggregationError> {
        self.checks_started.fetch_add(1, Ordering::SeqCst);
        while !*self.release.borrow() {
            if self.release.changed().await.is_err() {
                break;
            }
        }
        Ok(None)
    }

    async fn read_aggregation_set(
        &mut self,
        _queue: &str,
        _group: &str,
        _set_id: &str,
    ) -> Result<AggregationSet, AggregationError> {
        unreachable!("blocking test broker never returns a ready set")
    }

    async fn delete_aggregation_set(
        &mut self,
        _queue: &str,
        _group: &str,
        _set_id: &str,
    ) -> Result<(), AggregationError> {
        unreachable!("blocking test broker never returns a ready set")
    }

    async fn enqueue_aggregated(&mut self, _plan: &EnqueuePlan) -> Result<(), BrokerError> {
        unreachable!("blocking test broker never returns a ready set")
    }

    async fn reclaim_stale_aggregation_sets(
        &mut self,
        _queue: &str,
        _now: SystemTime,
    ) -> Result<(), AggregationError> {
        Ok(())
    }
}
