use apalis::prelude::*;

#[tokio::main]
async fn main() {
    let mut backend = MemoryStorage::new();
    backend.push(42).await.unwrap();

    async fn task(args: u32, task_id: TaskId, worker: WorkerContext) -> Result<(), BoxDynError> {
        assert_eq!(args, 42);

        let task = worker.get_task(&task_id.to_string())?;

        // Lets get the inner execution context
        let ctx = task.execution_context().unwrap();

        assert_eq!(ctx.attempt(), 1, "The current attempt should be one");
        assert_eq!(ctx.status(), Status::Running, "The task should be running");
        assert!(ctx.task_id().is_some(), "A task_id is needed");

        tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

        worker.stop()?;

        Ok(())
    }
    let ctx = WorkerContext::new("rango-tango");
    let worker = WorkerBuilder::new(&ctx)
        .backend(backend)
        .enable_tracing()
        .build(task);

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            println!("Worker Tasks: {:?}", ctx.tasks());
        }
    });
    worker.run().await.unwrap();
}
