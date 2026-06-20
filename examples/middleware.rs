use asynqrs::{HandlerError, ProcessingContext, ServeMux, Task};

fn main() {
    let _mux = ServeMux::new()
        .layer_hooks(
            |task: &Task, context: &ProcessingContext| -> Result<(), HandlerError> {
                println!(
                    "starting task={} queue={}",
                    task.task_type(),
                    context.queue_name()
                );
                Ok(())
            },
            |task: &Task,
             _context: &ProcessingContext,
             result: Result<(), HandlerError>|
             -> Result<(), HandlerError> {
                if result.is_err() {
                    eprintln!("task failed: {}", task.task_type());
                }
                result
            },
        )
        .route_fn(
            "email:welcome",
            |_task: &Task, _context: &ProcessingContext| -> Result<(), HandlerError> { Ok(()) },
        );
}
