use tokio::sync::watch;

/// Returns a shutdown receiver that is signaled when the process receives an
/// upstream termination signal.
///
/// Reference: Asynq v0.26.0 `Run` methods wait for OS signals before
/// gracefully shutting down:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/signals_unix.go>.
pub(crate) fn os_shutdown_receiver() -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if wait_for_shutdown_signal().await {
            let _ = shutdown_tx.send(true);
        }
    });
    shutdown_rx
}

/// Returns stop and shutdown receivers for server signal-driven `Run`.
///
/// Reference: Asynq v0.26.0 Unix server signal handling stops the processor
/// on SIGTSTP and gracefully shuts down on SIGTERM/SIGINT:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/signals_unix.go>.
pub(crate) fn server_signal_receivers() -> (watch::Receiver<bool>, watch::Receiver<bool>) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(wait_for_server_signals(stop_tx, shutdown_tx));
    (stop_rx, shutdown_rx)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> bool {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(_) => return tokio::signal::ctrl_c().await.is_ok(),
        };
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.is_ok(),
        _ = terminate.recv() => true,
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> bool {
    tokio::signal::ctrl_c().await.is_ok()
}

#[cfg(unix)]
async fn wait_for_server_signals(stop: watch::Sender<bool>, shutdown: watch::Sender<bool>) {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(_) => return shutdown_on_ctrl_c(stop, shutdown).await,
        };
    let mut terminal_stop =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(libc::SIGTSTP))
        {
            Ok(signal) => signal,
            Err(_) => return shutdown_on_ctrl_c(stop, shutdown).await,
        };

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if result.is_ok() {
                    signal_server_shutdown(&stop, &shutdown);
                }
                break;
            }
            _ = terminate.recv() => {
                signal_server_shutdown(&stop, &shutdown);
                break;
            }
            _ = terminal_stop.recv() => {
                let _ = stop.send(true);
            }
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_server_signals(stop: watch::Sender<bool>, shutdown: watch::Sender<bool>) {
    shutdown_on_ctrl_c(stop, shutdown).await
}

async fn shutdown_on_ctrl_c(stop: watch::Sender<bool>, shutdown: watch::Sender<bool>) {
    if tokio::signal::ctrl_c().await.is_ok() {
        signal_server_shutdown(&stop, &shutdown);
    }
}

fn signal_server_shutdown(stop: &watch::Sender<bool>, shutdown: &watch::Sender<bool>) {
    let _ = stop.send(true);
    let _ = shutdown.send(true);
}
