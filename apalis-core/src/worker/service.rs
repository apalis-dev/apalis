//! Worker service composition and task-function validation utilities.
//!
//! This module provides the types and traits used to combine a [`Backend`]
//! with a task-processing service. It also contains utilities for validating
//! task function signatures when the `test-utils` feature is enabled.

use tower_service::Service;

use crate::backend::Backend;

/// A worker service composed of a backend and a task-processing service.
///
/// `WorkerService` combines the [`Backend`] responsible for retrieving and
/// managing tasks with the [`Service`] responsible for processing them.
#[derive(Debug, Clone)]
pub struct WorkerService<Backend, Svc> {
    /// The backend used by the worker.
    pub backend: Backend,

    /// The service used to process tasks.
    pub service: Svc,
}

/// A trait for constructing a [`WorkerService`] from a backend.
///
/// Implementations define how a service is combined with a backend before
/// being passed to a worker.
///
/// # Type Parameters
///
/// * `B` - The backend supplied when constructing the worker service.
/// * `Svc` - The service used to process tasks.
///
/// # Associated Types
///
/// * [`Task`](Self::Task) - The task type processed by the worker.
/// * [`Backend`](Self::Backend) - The backend used by the resulting worker
///   service.
pub trait IntoWorkerService<B, Svc>
where
    B: Backend,
    Svc: Service<<Self::Backend as Backend>::Task>,
{
    /// The task type processed by the worker.
    type Task;

    /// The backend type used by the worker service.
    type Backend: Backend<Task = Self::Task>;

    /// Builds a worker service from the provided backend.
    fn into_service(self, backend: B) -> WorkerService<Self::Backend, Svc>;
}

/// Utilities for validating task function implementations.
///
/// This module provides helper functions for checking that task functions
/// conform to the signatures supported by worker services.
///
/// The module is available when the `test-utils` feature is enabled.
#[cfg(feature = "test-utils")]
pub mod task_fn_validator {
    use crate::backend::Backend;
    use crate::backend::BackendConfig;
    use crate::task::Task;
    use tower_service::Service;

    use crate::task::from_request::FromRequest;
    use crate::task::task_fn::TaskFn;

    /// Macro for implementing the check functions
    macro_rules! impl_check_fn {
        ($($num:tt => $($arg:ident),+);+ $(;)?) => {
            $(
                #[inline]
                #[doc = concat!("A helper for checking that the builder can build a worker with the provided service (", stringify!($num), " arguments)")]
                pub fn $num<
                    F, B, Args,
                    $($arg: FromRequest<Task<Args>>),+
                >(
                    _: F,
                ) where
                    TaskFn<F, Args,  ($($arg,)+)>: Service<Task<Args>>,
                    B: Backend + BackendConfig<Args = Args>
                {
                }
            )+
        };
    }

    impl_check_fn! {
        check_fn_1 => A1;
        check_fn_2 => A1, A2;
        check_fn_3 => A1, A2, A3;
        check_fn_4 => A1, A2, A3, A4;
        check_fn_5 => A1, A2, A3, A4, A5;
        check_fn_6 => A1, A2, A3, A4, A5, A6;
        check_fn_7 => A1, A2, A3, A4, A5, A6, A7;
        check_fn_8 => A1, A2, A3, A4, A5, A6, A7, A8;
        check_fn_9 => A1, A2, A3, A4, A5, A6, A7, A8, A9;
        check_fn_10 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10;
        check_fn_11 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11;
        check_fn_12 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12;
        check_fn_13 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13;
        check_fn_14 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14;
        check_fn_15 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15;
        check_fn_16 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16;
    }
}
