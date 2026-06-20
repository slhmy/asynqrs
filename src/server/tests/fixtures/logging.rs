use super::*;

#[derive(Default)]
pub(in crate::server::tests) struct RecordingLogger {
    pub(in crate::server::tests) calls: StdMutex<Vec<(String, String)>>,
}

impl Logger for RecordingLogger {
    fn debug(&self, args: fmt::Arguments<'_>) {
        self.calls
            .lock()
            .unwrap()
            .push(("debug".to_owned(), args.to_string()));
    }

    fn info(&self, args: fmt::Arguments<'_>) {
        self.calls
            .lock()
            .unwrap()
            .push(("info".to_owned(), args.to_string()));
    }

    fn warn(&self, args: fmt::Arguments<'_>) {
        self.calls
            .lock()
            .unwrap()
            .push(("warn".to_owned(), args.to_string()));
    }

    fn error(&self, args: fmt::Arguments<'_>) {
        self.calls
            .lock()
            .unwrap()
            .push(("error".to_owned(), args.to_string()));
    }

    fn fatal(&self, args: fmt::Arguments<'_>) {
        self.calls
            .lock()
            .unwrap()
            .push(("fatal".to_owned(), args.to_string()));
    }
}
