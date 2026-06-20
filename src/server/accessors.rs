use super::{Server, ServerRuntimeState};

impl<P, S> Server<P, S> {
    pub(crate) fn runtime_state(&self) -> ServerRuntimeState {
        self.runtime_state.clone()
    }
}
