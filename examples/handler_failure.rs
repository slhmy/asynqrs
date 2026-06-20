use asynqrs::{HandlerError, ProcessingContext, Task};

fn retryable_failure(_task: &Task, _context: &ProcessingContext) -> Result<(), HandlerError> {
    Err(HandlerError::failed("temporary provider error"))
}

fn archive_without_retry(_task: &Task, _context: &ProcessingContext) -> Result<(), HandlerError> {
    Err(HandlerError::skip_retry("archive without retry"))
}

fn revoke(_task: &Task, _context: &ProcessingContext) -> Result<(), HandlerError> {
    Err(HandlerError::revoke_task("revoke task"))
}

fn main() {
    let _ = retryable_failure as fn(&Task, &ProcessingContext) -> Result<(), HandlerError>;
    let _ = archive_without_retry as fn(&Task, &ProcessingContext) -> Result<(), HandlerError>;
    let _ = revoke as fn(&Task, &ProcessingContext) -> Result<(), HandlerError>;
}
