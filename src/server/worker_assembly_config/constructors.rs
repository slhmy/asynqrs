use crate::SystemClock;

use crate::processing::{DefaultIsFailure, NoopErrorHandler, NoopLeaseExtender, TaskExecutor};
use crate::server::WorkerAssembly;

impl<B, H, R> WorkerAssembly<B, H, R, SystemClock> {
    pub(crate) fn with_retry_delay(broker: B, handler: H, retry_delay: R) -> Self {
        Self::with_parts(broker, handler, retry_delay, SystemClock)
    }
}

impl<B, H, R, C> WorkerAssembly<B, H, R, C, DefaultIsFailure, NoopErrorHandler, NoopLeaseExtender> {
    pub fn with_parts(broker: B, handler: H, retry_delay: R, clock: C) -> Self {
        let executor = TaskExecutor::new(handler, retry_delay, DefaultIsFailure);
        Self::with_broker_executor_and_clock(broker, executor, clock)
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L> {
    pub fn with_parts_hooks_and_lease_extender(
        broker: B,
        handler: H,
        retry_delay: R,
        clock: C,
        is_failure: I,
        error_handler: E,
        lease_extender: L,
    ) -> Self {
        let executor = TaskExecutor::with_hooks_and_lease_extender(
            handler,
            retry_delay,
            is_failure,
            error_handler,
            lease_extender,
        );
        Self::with_broker_executor_and_clock(broker, executor, clock)
    }

    pub(in crate::server) fn with_broker_executor_and_clock(
        broker: B,
        executor: TaskExecutor<H, R, I, E, L>,
        clock: C,
    ) -> Self {
        WorkerAssembly::new(broker, executor, clock)
    }
}
