use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::task::Poll;
use std::time::SystemTime;

use tokio_util::sync::CancellationToken;

use crate::server::{LogLevel, Logger};
use crate::{ProcessingContext, Task};

use super::super::{Handler, HandlerError};
use crate::server::{log_processing_error, tokio_instant_for_system_time};

static PANIC_HOOK_LOCK: StdMutex<()> = StdMutex::new(());

type PanicLocation = Option<(String, u32)>;
type PanicCapture = (Box<dyn Any + Send>, PanicLocation);

pub(super) struct HandlerExecution<'a> {
    pub(super) task: &'a Task,
    pub(super) context: &'a ProcessingContext,
    pub(super) deadline: Option<SystemTime>,
    pub(super) now: SystemTime,
    pub(super) cancellation: CancellationToken,
    pub(super) logger: &'a Option<Arc<dyn Logger>>,
    pub(super) log_level: LogLevel,
}

pub(super) async fn perform<H>(
    handler: &mut H,
    execution: HandlerExecution<'_>,
) -> Result<(), HandlerError>
where
    H: Handler + Send,
{
    let HandlerExecution {
        task,
        context,
        deadline,
        now,
        cancellation,
        logger,
        log_level,
    } = execution;

    if deadline.is_some_and(|deadline| deadline <= now) {
        return Err(HandlerError::failed("context deadline exceeded"));
    }
    if cancellation.is_cancelled() {
        return Err(HandlerError::failed("context canceled"));
    }

    let future = handler.process_task(task, context);
    tokio::pin!(future);
    let caught = std::future::poll_fn(|cx| {
        poll_with_panic_location(|| future.as_mut().poll(cx)).unwrap_or_else(|(panic, location)| {
            let message = panic_message(panic.as_ref());
            // Reference: Asynq v0.26.0 logs recovered handler panics before
            // converting them into retryable panic errors:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L421-L447>.
            log_processing_error(
                logger,
                log_level,
                format_args!(
                    "recovering from panic. See the stack trace below for details:\n{}",
                    std::backtrace::Backtrace::force_capture()
                ),
            );
            let error = if let Some((file, line)) = location {
                HandlerError::panic_at(message, file, line)
            } else {
                HandlerError::panic(message)
            };
            Poll::Ready(Err(error))
        })
    });

    let deadline = deadline.and_then(|deadline| tokio_instant_for_system_time(deadline, now));
    tokio::pin!(caught);

    match deadline {
        Some(deadline) => {
            tokio::select! {
                result = &mut caught => result,
                _ = tokio::time::sleep_until(deadline) => {
                    Err(HandlerError::failed("context deadline exceeded"))
                }
                _ = cancellation.cancelled() => Err(HandlerError::failed("context canceled")),
            }
        }
        None => {
            tokio::select! {
                result = &mut caught => result,
                _ = cancellation.cancelled() => Err(HandlerError::failed("context canceled")),
            }
        }
    }
}

fn poll_with_panic_location<T>(poll: impl FnOnce() -> T) -> Result<T, PanicCapture> {
    let _guard = PANIC_HOOK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let panic_location = Arc::new(StdMutex::new(None));
    let hook_location = Arc::clone(&panic_location);
    let current_thread = std::thread::current().id();
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if std::thread::current().id() == current_thread {
            if let Some(location) = info.location() {
                if let Ok(mut panic_location) = hook_location.lock() {
                    *panic_location = Some((location.file().to_owned(), location.line()));
                }
            }
        }
    }));
    let result = catch_unwind(AssertUnwindSafe(poll));
    std::panic::set_hook(previous_hook);
    result.map_err(|panic| {
        let location = panic_location
            .lock()
            .map(|location| location.clone())
            .unwrap_or(None);
        (panic, location)
    })
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "task handler panicked".to_owned()
    }
}
