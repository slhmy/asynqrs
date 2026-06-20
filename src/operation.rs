//! Queue operation models shared across client, scheduler, broker, and server processing.

mod dequeue;
mod enqueue;
mod lifecycle;

pub use dequeue::{DequeueError, DequeuedTask};
pub use enqueue::{
    DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT, EnqueueOptions, EnqueuePlan, EnqueuePlanError, NO_DEADLINE,
    NO_TIMEOUT,
};
pub(crate) use lifecycle::ResultWrite;
pub use lifecycle::{
    ArchiveError, CancelError, CleanupError, CompleteError, ForwardError, LeaseError,
    LeaseExtension, RecoverError, RecoverResult, RequeueError, ResultError, ResultWriter,
    RetryError,
};
