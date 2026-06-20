use crate::Task;
use crate::task::TaskMessage;

pub(crate) fn task_from_message(message: &TaskMessage) -> Task {
    Task::with_headers(
        message.r#type.clone(),
        message.payload.clone(),
        message.headers.clone(),
    )
}
