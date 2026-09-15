//! Utilities for adapting async functions into a task handler.
//!
//! The [`task_fn`] helper and the [`TaskFn`] struct in this module allow you to wrap
//! async functions or closures into a [`TaskFn`] implementation, which can then be
//! used in service middleware pipelines or other components expecting a [`TaskFn`].
//!
//! This is particularly useful when building lightweight, composable services from plain
//! functions, including those with extracted arguments via [`FromRequest`].
//!
//! ## Features
//!
//! - Supports functions with up to 16 additional arguments beyond the core request.
//! - Automatically applies argument extraction using the [`FromRequest`] trait.
//! - Converts output to responses using the [`IntoResponse`] trait.
//! - Captures function argument types at compile time via generics for static dispatch.
//!
//!
//! ## Introduction
//!
//! The first argument of any task function is the type `Args` which is tied to the backend's task type.
//! Eg if you are writing a task for an email service, `Args` might be a struct `Email` that includes fields like `user_id`, `subject`, and `message`.
//!
//! A rule of thumb is to never store database models as task arguments.
//!
//! Instead of doing this:
//! ```rust
//! struct User {
//!     id: String,
//!     // other fields...
//! }
//! struct Email {
//!     user: User,
//!     subject: String,
//!     message: String,
//! }
//! ```
//! Do this:
//! ```
//! struct Email {
//!     user_id: String,
//!     subject: String,
//!     message: String,
//! }
//! ```
//!
//! All the primitive types (e.g. `String`, `u32`) can be used directly as task arguments.
//!
//! **Note:**
//!
//! > *Some backends like `apalis-cron` offer a specific `Args` type (Tick) for cron jobs while most others like `postgres` use a more generic `Args` type.*
//!
//! A guide for extracting complex information from tasks using [`FromRequest`] is available in [step 3](#3-implementing-custom-argument-extraction-with-fromrequest).
//!
//! ## Getting started
//!
//! Task handlers are async functions that process a task. You can use the [`task_fn`] helper
//! to wrap your handler into a service.
//!
//! ```rust
//! # use apalis_core::task::data::Data;
//! #[derive(Clone)]
//! struct State;
//!
//! // A simple handler that takes an id and injected state
//! async fn handler(id: u32, state: Data<State>) -> String {
//!     format!("Got id {} with state", id)
//! }
//! ```
//! You would need to inject the state in your worker builder:
//!
//! ```rs
//! let worker = WorkerBuilder::new()
//!     .backend(in_memory)
//!     .data(State)
//!     .build(handler);
//! ```
//!
//! ## Dependency Injection
//!
//! `apalis-core` supports default injection for common types in your handler arguments, such as:
//! - [`WorkerContext`]: Worker context
//! - [`TaskContext`]: The tasks context including the execution context
//! - [`Attempt`]: Information about the current attempt
//! - [`Data<T>`]: Injected data/state
//! - [`TaskId`]: The unique ID of the task
//!
//! Example:
//! ```rust
//! # use apalis_core::task::{attempt::Attempt, data::Data, task_id::TaskId, context::TaskContext};
//! #[derive(Clone)]
//! struct State;
//!
//! async fn process_task(_: u32, attempt: Attempt, ctx: TaskContext, id: TaskId) -> String {
//!     format!("Attempt {} for task {} with elapsed: {:?}", attempt.current(), id, ctx.elapsed())
//! }
//! ```
//!
//!
//! ## Custom argument extraction with [`FromRequest`]
//!
//! You can extract custom types from the request by implementing [`FromRequest`].
//!
//! Suppose you have a task to send emails, and you want to automatically extract a `User` from the task's `user_id`:
//!
//! ```rust
//! struct Email {
//!     user_id: String,
//!     subject: String,
//!     message: String,
//! }
//!
//! // Implement FromRequest for User
//! # use apalis_core::task::from_request::FromRequest;
//! # use apalis_core::task::Task;
//! # use apalis_core::error::BoxDynError;
//! # struct User {
//! #    id: String,
//! #    // other fields...
//! # }
//!
//! impl FromRequest<Task<Email>> for User {
//!     type Error = BoxDynError;
//!     async fn from_request(req: &Task<Email>) -> Result<Self, Self::Error> {
//!         let user_id = req.args.user_id.clone();
//!         // Simulate fetching user from DB
//!         Ok(User { id: user_id })
//!     }
//! }
//!
//! // Now your handler can take User directly
//! async fn send_email(email: Email, user: User) -> Result<(), BoxDynError> {
//!     // Use email and user
//!     Ok(())
//! }
//! ```
//!
//! ## How It Works
//!
//! - [`task_fn`] wraps your handler into a [`TaskFn`] service.
//! - Arguments are extracted using [`FromRequest`].
//! - DI types are injected automatically.
//! - The handler's output is converted to a response using [`IntoResponse`].
//!
//! [`task_fn`]: crate::task::task_fn::task_fn
//! [`TaskFn`]: crate::task::task_fn::TaskFn
//! [`FromRequest`]: crate::task::from_request::FromRequest
//! [`IntoResponse`]: crate::task::into_response::IntoResponse
//! [`Attempt`]: crate::task::attempt::Attempt
//! [`Data<T>`]: crate::task::data::Data
//! [`WorkerContext`]: crate::worker::context::WorkerContext
//! [`TaskContext`]: crate::task::context::TaskContext
//! [`TaskId`]: crate::task::task_id::TaskId

use crate::backend::finalize::FinalizeBackend;
use crate::backend::{Backend, BackendConfig};
use crate::error::BoxDynError;
use crate::task::Task;
use crate::worker::service::{IntoWorkerService, WorkerService};
use futures_util::FutureExt;
use futures_util::future::Map;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};
use tower_service::Service;

use crate::task::{from_request::FromRequest, into_response::IntoResponse};

/// A helper method to build a [`TaskFn`] from an async function or closure.
///
/// # Example
/// ```rust
/// # use apalis_core::task::data::Data;
/// #[derive(Clone)]
/// struct State {
///     // db: Arc<DatabaseConnection>,
/// }
/// async fn handler(id: u32, state: Data<State>) -> String {
///     format!("Got id {} with state", id)
/// }   
///```
/// This method can take functions with up to 16 additional arguments beyond the core request.
///
/// See Also:
///
/// - [`FromRequest`]
/// - [`IntoResponse`]
pub fn task_fn<F, Args, FnArgs>(f: F) -> TaskFn<F, Args, FnArgs> {
    TaskFn {
        f,
        req: PhantomData,
        fn_args: PhantomData,
    }
}

/// An executable service implemented by a closure.
///
/// See [`task_fn`] for more details.
pub struct TaskFn<F, Args, FnArgs> {
    f: F,
    req: PhantomData<Args>,
    fn_args: PhantomData<FnArgs>,
}

impl<T: Copy, Args, FnArgs> Copy for TaskFn<T, Args, FnArgs> {}

impl<T: Clone, Args, FnArgs> Clone for TaskFn<T, Args, FnArgs> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            req: PhantomData,
            fn_args: PhantomData,
        }
    }
}

impl<T, Args, FnArgs> fmt::Debug for TaskFn<T, Args, FnArgs> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskFn")
            .field("f", &std::any::type_name::<T>())
            .field(
                "req",
                &format_args!("PhantomData<Task<{}>>", std::any::type_name::<Args>(),),
            )
            .field(
                "fn_args",
                &format_args!("PhantomData<{}>", std::any::type_name::<FnArgs>()),
            )
            .finish()
    }
}

/// The Future returned from [`TaskFn`] service.
type FnFuture<F, O, R, E> = Map<F, fn(O) -> std::result::Result<R, E>>;

macro_rules! impl_service_fn {
    ($($K:ident),+) => {
        #[allow(unused_parens)]
        impl<T, F, Args: Send + 'static, R, $($K),+> Service<Task<Args>> for TaskFn<T, Args,  ($($K),+)>
        where
            T: FnMut(Args, $($K),+) -> F + Send + Clone + 'static,
            F: Future + Send,
            F::Output: IntoResponse<Output = R>,
            $(
                $K: FromRequest<Task<Args>> + Send,
                < $K as FromRequest<Task<Args>> >::Error: std::error::Error + 'static + Send + Sync,
            )+
        {
            type Response = R;
            type Error = BoxDynError;
            type Future = futures_util::future::BoxFuture<'static, Result<R, BoxDynError>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, task: Task<Args>) -> Self::Future {
                let mut svc = self.f.clone();
                #[allow(non_snake_case)]
                let fut = async move {
                    #[allow(clippy::double_parens)]
                    let results: Result<($($K),+), BoxDynError> = { Ok(($($K::from_request(&task).await.map_err(|e| Box::new(e) as BoxDynError)?),+)) };
                    match results {
                        Ok(($($K),+)) => {
                            let req = task.args;
                            (svc)(req, $($K),+).map(F::Output::into_response).await
                        }
                        Err(e) => Err(e),
                    }
                };
                fut.boxed()
            }
        }

        #[allow(unused_parens)]
        impl<T, Args,  F, R, B, O, $($K),+>
            IntoWorkerService<B, TaskFn<T, Args,  ($($K),+)>> for T
        where
            B: Backend + BackendConfig<Args = Args>,
            B::Kind: FinalizeBackend<B, Args, Backend = O>,
            O: Backend<Task = Task<Args>>,
            T: FnMut(Args, $($K),+) -> F + Send + Clone + 'static,
            F: Future + Send,
            Args: Send + 'static,
            F::Output: IntoResponse<Output = R>,
            TaskFn<T, Args,  ($($K),+)>: Service<O::Task>,

            $(
                $K: FromRequest<Task<Args>> + Send,
                < $K as FromRequest<Task<Args>> >::Error: std::error::Error + 'static + Send + Sync,
            )+
        {
            type Backend = O;
            type Task = Task<Args>;

            fn into_service(self, backend: B) -> WorkerService<O, TaskFn<T, Args,  ($($K),+)>> {
                let backend = B::Kind::finalize(backend);
                WorkerService {
                    backend,
                    service: task_fn(self)
                }
            }
        }
    };
}

impl<T, F, Args, R> Service<Task<Args>> for TaskFn<T, Args, ()>
where
    T: FnMut(Args) -> F,
    F: Future,
    F::Output: IntoResponse<Output = R>,
{
    type Response = R;
    type Error = BoxDynError;
    type Future = FnFuture<F, F::Output, R, BoxDynError>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        let fut = (self.f)(task.args);

        fut.map(F::Output::into_response)
    }
}

impl<T, Args, F, R, B, O> IntoWorkerService<B, TaskFn<T, Args, ()>> for T
where
    T: FnMut(Args) -> F,
    F: Future,
    F::Output: IntoResponse<Output = R>,
    B: Backend + BackendConfig<Args = Args>,
    B::Kind: FinalizeBackend<B, Args, Backend = O>,
    O: Backend<Task = Task<Args>>,
    Args: Send,
    TaskFn<T, Args, ()>: Service<O::Task>,
{
    type Backend = O;
    type Task = Task<Args>;

    fn into_service(self, backend: B) -> WorkerService<O, TaskFn<T, Args, ()>> {
        let backend = B::Kind::finalize(backend);
        WorkerService {
            backend,
            service: task_fn(self),
        }
    }
}

impl<Args, S, B, O> IntoWorkerService<B, S> for S
where
    B: Backend + BackendConfig<Args = Args>,
    B::Kind: FinalizeBackend<B, Args, Backend = O>,
    O: Backend<Task = Task<Args>>,
    S: Service<O::Task>,
{
    type Backend = O;
    type Task = Task<Args>;
    fn into_service(self, backend: B) -> WorkerService<O, S> {
        let backend = B::Kind::finalize(backend);
        WorkerService {
            backend,
            service: self,
        }
    }
}

impl_service_fn!(A);
impl_service_fn!(A1, A2);
impl_service_fn!(A1, A2, A3);
impl_service_fn!(A1, A2, A3, A4);
impl_service_fn!(A1, A2, A3, A4, A5);
impl_service_fn!(A1, A2, A3, A4, A5, A6);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14);
impl_service_fn!(
    A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15
);
impl_service_fn!(
    A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16
);
