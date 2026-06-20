use std::collections::HashMap;

use async_trait::async_trait;

use super::{
    Handler, HandlerError, HandlerFunc, TaskMiddleware, TaskMiddlewareFn, TaskMiddlewareHooks,
    TypedHandlerFunc, task_middleware_hooks,
};
use crate::{ProcessingContext, Task, TypedTaskPayload};

/// Multiplexes tasks to handlers by task type pattern.
///
/// Reference: Asynq v0.26.0 public `ServeMux` matches exact task types first,
/// then the longest registered prefix, and falls back to a not-found handler:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L31-L113>.
#[derive(Default)]
pub struct ServeMux {
    handlers: HashMap<String, Box<dyn Handler + Send>>,
    patterns: Vec<String>,
    layers: Vec<Box<dyn TaskMiddleware + Send>>,
    not_found: NotFoundHandler,
}

impl ServeMux {
    /// Reference: Asynq v0.26.0 public `NewServeMux` constructor:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L40-L43>.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle<H>(&mut self, pattern: impl Into<String>, handler: H)
    where
        H: Handler + Send + 'static,
    {
        let pattern = pattern.into();
        if pattern.trim().is_empty() {
            panic!("asynq: invalid pattern");
        }
        if self.handlers.contains_key(&pattern) {
            panic!("asynq: multiple registrations for {pattern}");
        }

        let index = self
            .patterns
            .iter()
            .position(|candidate| candidate.len() < pattern.len())
            .unwrap_or(self.patterns.len());
        self.patterns.insert(index, pattern.clone());
        self.handlers.insert(pattern, Box::new(handler));
    }

    pub fn handle_fn<F>(&mut self, pattern: impl Into<String>, handler: F)
    where
        F: FnMut(&Task, &ProcessingContext) -> Result<(), HandlerError> + Send + 'static,
    {
        self.handle(pattern, HandlerFunc(handler));
    }

    pub fn handle_typed<P, F>(&mut self, handler: F)
    where
        P: TypedTaskPayload + Send + 'static,
        F: FnMut(P, &ProcessingContext) -> Result<(), HandlerError> + Send + 'static,
    {
        self.handle(P::TASK_TYPE, TypedHandlerFunc::<P, F>::new(handler));
    }

    pub fn route<H>(mut self, pattern: impl Into<String>, handler: H) -> Self
    where
        H: Handler + Send + 'static,
    {
        self.handle(pattern, handler);
        self
    }

    pub fn route_fn<F>(mut self, pattern: impl Into<String>, handler: F) -> Self
    where
        F: FnMut(&Task, &ProcessingContext) -> Result<(), HandlerError> + Send + 'static,
    {
        self.handle_fn(pattern, handler);
        self
    }

    pub fn route_typed<P, F>(mut self, handler: F) -> Self
    where
        P: TypedTaskPayload + Send + 'static,
        F: FnMut(P, &ProcessingContext) -> Result<(), HandlerError> + Send + 'static,
    {
        self.handle_typed::<P, F>(handler);
        self
    }

    pub fn use_layer<M>(&mut self, layer: M)
    where
        M: TaskMiddleware + Send + 'static,
    {
        self.layers.push(Box::new(layer));
    }

    pub fn use_layers<I, M>(&mut self, layers: I)
    where
        I: IntoIterator<Item = M>,
        M: TaskMiddleware + Send + 'static,
    {
        for layer in layers {
            self.use_layer(layer);
        }
    }

    pub fn layer<M>(mut self, layer: M) -> Self
    where
        M: TaskMiddleware + Send + 'static,
    {
        self.use_layer(layer);
        self
    }

    pub fn layer_fn<F>(self, middleware: F) -> Self
    where
        TaskMiddlewareFn<F>: TaskMiddleware + Send + 'static,
    {
        self.layer(TaskMiddlewareFn(middleware))
    }

    pub fn layer_hooks<B, A>(self, before: B, after: A) -> Self
    where
        TaskMiddlewareHooks<B, A>: TaskMiddleware + Send + 'static,
    {
        self.layer(task_middleware_hooks(before, after))
    }

    pub fn layers<I, M>(mut self, layers: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: TaskMiddleware + Send + 'static,
    {
        self.use_layers(layers);
        self
    }

    pub fn matching_pattern(&self, type_name: &str) -> Option<&str> {
        if let Some(pattern) = self
            .patterns
            .iter()
            .find(|pattern| pattern.as_str() == type_name)
        {
            return Some(pattern);
        }

        self.patterns
            .iter()
            .find(|pattern| type_name.starts_with(pattern.as_str()))
            .map(String::as_str)
    }

    /// Reference: Asynq v0.26.0 public `ServeMux.Handler` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L53-L71>.
    pub fn handler(&mut self, task: &Task) -> (ServeMuxMatchedHandler<'_>, String) {
        let pattern = self.matching_pattern(task.type_name()).map(str::to_owned);
        let (handler, pattern): (&mut (dyn Handler + Send), String) = match pattern {
            Some(pattern) => (
                self.handlers
                    .get_mut(&pattern)
                    .expect("ServeMux matched pattern without registered handler")
                    .as_mut(),
                pattern,
            ),
            None => (&mut self.not_found, String::new()),
        };
        (
            ServeMuxMatchedHandler {
                layers: &mut self.layers,
                handler,
            },
            pattern,
        )
    }
}

#[async_trait]
impl Handler for ServeMux {
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        let (mut handler, _) = self.handler(task);
        handler.process_task(task, context).await
    }
}

/// Handler selected by `ServeMux::Handler`.
///
/// Reference: Asynq v0.26.0 public `ServeMux.Handler` returns the selected
/// handler after applying registered middleware wrappers:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L53-L71>.
pub struct ServeMuxMatchedHandler<'a> {
    layers: &'a mut [Box<dyn TaskMiddleware + Send>],
    handler: &'a mut (dyn Handler + Send),
}

#[async_trait]
impl Handler for ServeMuxMatchedHandler<'_> {
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        let Some((layer, remaining)) = self.layers.split_first_mut() else {
            return self.handler.process_task(task, context).await;
        };
        let mut next = ServeMuxMatchedHandler {
            layers: remaining,
            handler: self.handler,
        };
        layer.process_task(task, context, &mut next).await
    }
}

/// Handler that returns a not-found error for every task.
///
/// Reference: Asynq v0.26.0 public `NotFound` and `NotFoundHandler`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L105-L113>.
#[derive(Debug, Clone, Copy, Default)]
pub struct NotFoundHandler;

/// Returns the upstream not-found handler error for a task.
///
/// Reference: Asynq v0.26.0 public `NotFound`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L105-L107>.
pub fn not_found(task: &Task) -> HandlerError {
    HandlerError::handler_not_found(task.type_name())
}

pub fn not_found_handler() -> NotFoundHandler {
    NotFoundHandler
}

#[async_trait]
impl Handler for NotFoundHandler {
    async fn process_task(
        &mut self,
        task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        Err(not_found(task))
    }
}
