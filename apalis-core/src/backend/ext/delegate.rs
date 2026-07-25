//! A helper macros to delegate `Backend` trait methods from a wrapper type to its inner backend field.

/// This macro is used to implement the `Sink` trait for a wrapper type that contains a backend field. It delegates the `Sink` methods to the inner backend field, allowing the wrapper type to behave like a `Sink` without having to manually implement each method.
#[macro_export]
macro_rules! delegate_sink {
    ($wrapper:ident<$backend:ident $(, $rest:ident)* $(,)?>, $field:ident) => {
        impl<$backend, $($rest,)* T, Err> Sink<T> for $wrapper<$backend, $($rest,)*>
        where
            $backend: Sink<T, Error = Err> + Unpin,
            $($rest: Unpin,)*
        {
            type Error = Err;

            fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
                self.get_mut().$field.start_send_unpin(item)
            }

            fn poll_ready(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                self.get_mut().$field.poll_ready_unpin(cx)
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                self.get_mut().$field.poll_flush_unpin(cx)
            }

            fn poll_close(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                self.get_mut().$field.poll_close_unpin(cx)
            }
        }
    };
}
/// Delegates FetchById, Update, Reschedule, Vacuum, ResumeById,
/// ResumeAbandoned, RegisterWorker, and WaitForCompletion to a named field
/// in one call. The caller supplies shared extra bounds, and optionally a
/// `wrap` expression applied to every delegated method's `Result`.
///
/// `wrap` receives the `Result<T, B::Error>` returned by the inner call and
/// must produce the wrapper's own `Result<T, Self::Error>` — use it for
/// `.map_err(...)`, logging, retries, or any other postprocessing.
///
/// ```ignore
/// // plain forwarding
/// delegate_expose!(
///     impl<B> for RawDataBackend<B>
///     where {
///         B: Send + 'static,
///         B::Compact: Send + Clone + 'static,
///     }
///     => inner
/// );
///
/// // with postprocessing
/// delegate_expose!(
///     impl<B, F, E2> for MapErr<B, F>
///     where {
///         B: Backend,
///         F: Fn(B::Error) -> E2,
///         E2: std::error::Error + Send + Sync + 'static,
///     }
///     => backend,
///     wrap = |this, result| result.map_err(|err| (this.f)(err))
/// );
/// ```
#[macro_export]
macro_rules! delegate_expose {
    (
        impl<$($generic:ident),+> for $wrapper:ty
        where { $($bound:tt)+ }
        => $field:ident
    ) => {
        $crate::delegate_expose!(
            impl<$($generic),+> for $wrapper
            where { $($bound)+ }
            => $field,
            wrap = |this, result| result
        );
    };
    (
        impl<$($generic:ident),+> for $wrapper:ty
        where { $($bound:tt)+ }
        => $field:ident,
        wrap = |$this:ident, $result:ident| $wrap_body:expr
    ) => {
        impl<$($generic),+> FetchById for $wrapper
        where
            B: FetchById,
            B::Compact: Send,
            $($bound)+
        {
            async fn fetch_by_id(
                &mut self,
                task_id: &$crate::task::task_id::TaskId<Self::Id>,
            ) -> Result<Option<Task<B::Compact, Self::Id>>, Self::Error> {
                let $result = self.$field.fetch_by_id(task_id).await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }

        impl<$($generic),+> Update for $wrapper
        where
            B: Update + Send,
            B::Compact: Send,
            $($bound)+
        {
            async fn update(
                &mut self,
                task: Task<Self::Compact, Self::Id>,
            ) -> Result<(), Self::Error> {
                let $result = self.$field.update(task).await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }

        impl<$($generic),+> Reschedule for $wrapper
        where
            B: Reschedule,
            B::Compact: Send,
            $($bound)+
        {
            async fn reschedule(
                &mut self,
                task: Task<Self::Compact, Self::Id>,
                wait: std::time::Duration,
            ) -> Result<(), Self::Error> {
                let $result = self.$field.reschedule(task, wait).await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }

        impl<$($generic),+> Vacuum for $wrapper
        where
            B: Vacuum,
            $($bound)+
        {
            async fn vacuum(&mut self) -> Result<usize, Self::Error> {
                let $result = self.$field.vacuum().await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }

        impl<$($generic),+> ResumeById for $wrapper
        where
            B: ResumeById,
            $($bound)+
        {
            async fn resume_by_id(
                &mut self,
                id: TaskId<Self::Id>,
            ) -> Result<bool, Self::Error> {
                let $result = self.$field.resume_by_id(id).await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }

        impl<$($generic),+> ResumeAbandoned for $wrapper
        where
            B: ResumeAbandoned,
            $($bound)+
        {
            async fn resume_abandoned(&mut self) -> Result<usize, Self::Error> {
                let $result = self.$field.resume_abandoned().await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }

        impl<$($generic),+> RegisterWorker for $wrapper
        where
            B: RegisterWorker,
            $($bound)+
        {
            async fn register_worker(&mut self, worker_id: String) -> Result<(), Self::Error> {
                let $result = self.$field.register_worker(worker_id).await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }

        impl<$($generic),+> WaitForCompletion for $wrapper
        where
            B: WaitForCompletion + Sync,
            B::Compact: 'static,
            $($bound)+
        {
            type ResultStream = futures_core::stream::BoxStream<'static, Result<TaskResult<Self::Compact, Self::Id>, Self::Error>>;

            fn wait_for(
                &self,
                task_ids: impl IntoIterator<Item = TaskId<Self::Id>>,
            ) -> Self::ResultStream {
                use futures_util::StreamExt;
                let $result = self.$field.wait_for(task_ids);
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body.boxed()
            }

            async fn check_status(
                &self,
                task_ids: impl IntoIterator<Item = TaskId<Self::Id>> + Send,
            ) -> Result<Vec<TaskResult<Self::Compact, Self::Id>>, Self::Error> {
                let $result = self.$field.check_status(task_ids).await;
                #[allow(unused_variables)]
                let $this = &*self;
                $wrap_body
            }
        }
    };
}
