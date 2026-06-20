use super::*;

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct RecordingSleeper {
    pub(in crate::server::tests) durations: Arc<Mutex<Vec<Duration>>>,
}

#[async_trait]
impl Sleeper for RecordingSleeper {
    async fn sleep(&mut self, duration: Duration) {
        self.durations.lock().await.push(duration);
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct RecordingCancellationListener {
    pub(in crate::server::tests) starts: Arc<Mutex<usize>>,
    pub(in crate::server::tests) stops: Arc<Mutex<usize>>,
    pub(in crate::server::tests) events: Arc<Mutex<Vec<&'static str>>>,
}

impl CancellationListener for RecordingCancellationListener {
    fn run_until_stopped(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<usize, ServerError>> {
        let listener = self.clone();
        tokio::spawn(async move {
            *listener.starts.lock().await += 1;
            listener.events.lock().await.push("listener-start");
            loop {
                if *shutdown.borrow() {
                    break;
                }
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            *listener.stops.lock().await += 1;
            listener.events.lock().await.push("listener-stop");
            Ok(0)
        })
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct RecordingAggregationRunner {
    pub(in crate::server::tests) starts: Arc<Mutex<usize>>,
    pub(in crate::server::tests) stops: Arc<Mutex<usize>>,
    pub(in crate::server::tests) events: Arc<Mutex<Vec<&'static str>>>,
}

impl AggregationRunner for RecordingAggregationRunner {
    fn run_until_stopped(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<AggregatorRun, ServerError>> {
        let runner = self.clone();
        tokio::spawn(async move {
            *runner.starts.lock().await += 1;
            runner.events.lock().await.push("aggregator-start");
            loop {
                if *shutdown.borrow() {
                    break;
                }
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            *runner.stops.lock().await += 1;
            runner.events.lock().await.push("aggregator-stop");
            Ok(AggregatorRun::default())
        })
    }
}

pub(in crate::server::tests) async fn wait_until<F, Fut>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(timeout, async {
        loop {
            if condition().await {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
