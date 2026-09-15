use std::time::Duration;

use apalis::prelude::*;
use tokio::select;

#[tokio::main]
async fn main() {
    let mut backend = MemoryStorage::new();
    backend.push(42).await.unwrap();

    async fn task(args: u32, task: TaskContext, worker: WorkerContext) -> Result<(), BoxDynError> {
        assert_eq!(args, 42);

        // Lets get the inner execution context
        let ctx = task.execution_context().unwrap();

        assert_eq!(ctx.attempt(), 1, "The current attempt should be one");
        assert_eq!(ctx.status(), Status::Running, "The task should be running");
        assert!(ctx.task_id().is_none(), "A task_id is needed");

        let t = task.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
            t.cancel().unwrap();
        });

        tokio::spawn(async move {
            // Wait for either cancellation or a very long time
            select! {
                _ = task.executed() => {
                    // The task was cancelled
                    assert!(task.elapsed() > Duration::from_secs(3));
                    assert!(task.elapsed() < Duration::from_millis(3100));

                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                    assert_eq!(
                        ctx.status(),
                        Status::Killed,
                        "The task should be killed"
                    );

                    worker.stop().unwrap();
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(9999)) => {
                     unreachable!("Task should be cancelled")
                }
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;

        unreachable!("We never get here because our task is cancelled");
    }
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .build(task);
    worker.run().await.unwrap();
}
