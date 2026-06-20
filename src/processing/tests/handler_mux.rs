use super::*;

fn test_processing_context() -> ProcessingContext {
    ProcessingContext::for_task(
        None,
        tokio_util::sync::CancellationToken::new(),
        "task-id",
        "critical",
        0,
        25,
    )
}

mod adapters;
mod errors;
mod fixtures;
mod mux;
