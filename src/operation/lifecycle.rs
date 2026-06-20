mod errors;
mod lease;
mod recover;
mod result;

pub use errors::{
    ArchiveError, CancelError, CleanupError, CompleteError, ForwardError, LeaseError, RecoverError,
    RequeueError, RetryError,
};
pub use lease::LeaseExtension;
pub use recover::RecoverResult;
pub(crate) use result::ResultWrite;
pub use result::{ResultError, ResultWriter};

#[cfg(test)]
mod tests;
