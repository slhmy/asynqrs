use asynqrs::{HandlerError, ProcessingContext, serve_mux};

struct NotPayload;

fn main() {
    let _mux = serve_mux! {
        NotPayload => |_payload: NotPayload, _context: &ProcessingContext| -> Result<(), HandlerError> {
            Ok(())
        },
    };
}
