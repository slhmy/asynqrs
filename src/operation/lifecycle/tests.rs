use super::*;
use bytes::Bytes;

fn result_writer_channel(
    task_id: impl Into<String>,
) -> (
    ResultWriter,
    tokio::sync::mpsc::UnboundedReceiver<ResultWrite>,
) {
    ResultWriter::channel_with_context(
        task_id,
        tokio_util::sync::CancellationToken::new(),
        None,
        None,
    )
}

#[test]
fn result_writer_exposes_associated_task_id() {
    let (writer, _receiver) = result_writer_channel("task-id");

    assert_eq!(writer.task_id(), "task-id");
}

#[test]
fn result_writer_write_method_matches_upstream_name() {
    let (writer, mut receiver) = result_writer_channel("task-id");

    let written = writer.write(b"handler-result".to_vec()).unwrap();

    assert_eq!(written, b"handler-result".len());
    let write = receiver.try_recv().unwrap();
    assert_eq!(write.data.as_ref(), b"handler-result");
    assert!(write.ack.is_none());
}

#[test]
fn result_writer_accepts_bytes_without_forcing_vec_at_call_boundary() {
    let (writer, mut receiver) = result_writer_channel("task-id");

    let written = writer.write(Bytes::from_static(b"handler-result")).unwrap();

    assert_eq!(written, b"handler-result".len());
    let write = receiver.try_recv().unwrap();
    assert_eq!(write.data, Bytes::from_static(b"handler-result"));
    assert!(write.ack.is_none());
}

#[test]
fn result_writer_implements_standard_write_trait() {
    let (mut writer, mut receiver) = result_writer_channel("task-id");

    let written = std::io::Write::write(&mut writer, b"handler-result").unwrap();
    std::io::Write::flush(&mut writer).unwrap();

    assert_eq!(written, b"handler-result".len());
    let write = receiver.try_recv().unwrap();
    assert_eq!(write.data.as_ref(), b"handler-result");
    assert!(write.ack.is_none());
}

#[tokio::test]
async fn result_writer_write_async_waits_for_acknowledgement() {
    let (writer, mut receiver) = result_writer_channel("task-id");

    let write = tokio::spawn(async move { writer.write_async(b"handler-result".to_vec()).await });
    let request = receiver.recv().await.unwrap();
    assert_eq!(request.data.as_ref(), b"handler-result");
    request
        .ack
        .unwrap()
        .send(Ok(b"handler-result".len()))
        .unwrap();

    assert_eq!(write.await.unwrap().unwrap(), b"handler-result".len());
}

#[tokio::test]
async fn result_writer_write_async_reports_acknowledged_errors() {
    let (writer, mut receiver) = result_writer_channel("task-id");

    let write = tokio::spawn(async move { writer.write_async(b"handler-result".to_vec()).await });
    let request = receiver.recv().await.unwrap();
    request
        .ack
        .unwrap()
        .send(Err(ResultError::Other("redis down".to_owned())))
        .unwrap();

    assert_eq!(
        write.await.unwrap().unwrap_err(),
        ResultError::Other("redis down".to_owned())
    );
}

#[test]
fn result_writer_standard_write_reports_closed_channel() {
    let (mut writer, receiver) = result_writer_channel("task-id");
    drop(receiver);

    let error = std::io::Write::write(&mut writer, b"handler-result").unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(
        error.to_string(),
        "failed to write task result: result writer is closed"
    );
}

#[test]
fn result_writer_closed_channel_uses_upstream_write_error_prefix() {
    let (writer, receiver) = result_writer_channel("task-id");
    drop(receiver);

    let error = writer.write(b"handler-result".to_vec()).unwrap_err();

    assert_eq!(
        error,
        ResultError::WriteFailed("result writer is closed".to_owned())
    );
    assert!(error.is_writer_closed());
    assert_eq!(
        error.to_string(),
        "failed to write task result: result writer is closed"
    );
}

#[test]
fn result_writer_write_reports_cancelled_context_before_sending() {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let (writer, mut receiver) =
        ResultWriter::channel_with_context("task-id", cancellation.clone(), None, None);
    cancellation.cancel();

    let error = writer.write(b"handler-result".to_vec()).unwrap_err();

    assert_eq!(
        error,
        ResultError::WriteFailed("context canceled".to_owned())
    );
    assert!(error.is_context_cancelled());
    assert_eq!(
        error.to_string(),
        "failed to write task result: context canceled"
    );
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn result_writer_write_async_reports_cancelled_context_before_sending() {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let (writer, mut receiver) =
        ResultWriter::channel_with_context("task-id", cancellation.clone(), None, None);
    cancellation.cancel();

    let error = writer
        .write_async(b"handler-result".to_vec())
        .await
        .unwrap_err();

    assert_eq!(
        error,
        ResultError::WriteFailed("context canceled".to_owned())
    );
    assert!(error.is_context_cancelled());
    assert_eq!(
        error.to_string(),
        "failed to write task result: context canceled"
    );
    assert!(receiver.try_recv().is_err());
}

#[test]
fn result_writer_write_reports_expired_deadline_before_sending() {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let (writer, mut receiver) = ResultWriter::channel_with_context(
        "task-id",
        cancellation,
        None,
        Some(tokio::time::Instant::now() - std::time::Duration::from_nanos(1)),
    );

    let error = writer.write(b"handler-result".to_vec()).unwrap_err();

    assert_eq!(
        error,
        ResultError::WriteFailed("context deadline exceeded".to_owned())
    );
    assert!(error.is_context_deadline_exceeded());
    assert_eq!(
        error.to_string(),
        "failed to write task result: context deadline exceeded"
    );
    assert!(receiver.try_recv().is_err());
}
