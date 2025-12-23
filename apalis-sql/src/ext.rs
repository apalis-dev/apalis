use apalis_core::task::builder::TaskBuilder;

use crate::context::SqlContext;

/// Extension traits for [`TaskBuilder`]
pub trait TaskBuilderExt {
    /// Set the max number of attempts for the task being built.
    #[must_use]
    fn max_attempts(self, attempts: u32) -> Self;

    /// Set the priority for the task being built.
    #[must_use]
    fn priority(self, priority: i32) -> Self;
}

impl<Args, Pool, IdType> TaskBuilderExt for TaskBuilder<Args, SqlContext<Pool>, IdType> {
    fn max_attempts(mut self, attempts: u32) -> Self {
        self.ctx = self.ctx.with_max_attempts(attempts as i32);
        self
    }

    fn priority(mut self, priority: i32) -> Self {
        self.ctx = self.ctx.with_priority(priority);
        self
    }
}
