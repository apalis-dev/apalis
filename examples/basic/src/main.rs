#![allow(missing_docs)]
use std::time::Duration;

use apalis::{layers::catch_panic::PanicError, prelude::*};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let cpu_n = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);

    let mut backend = MemoryStorage::new();
    backend.push(42).await.unwrap();

    async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(task, 42);
        worker.stop()?;
        Ok(())
    }
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .rate_limit(100, Duration::from_secs(1)) // 100 jobs every second
        .retry(
            RetryPolicy::retries(3) // Retry three times max
                .retry_if(|e: &BoxDynError| e.downcast_ref::<PanicError>().is_none()), // Do not retry panics
        )
        .catch_panic() // Catch any panics
        .parallelize(tokio::spawn) // Each job future will be spawned via tokio
        .enable_tracing() // Use the `tracing` crate
        .concurrency(cpu_n) // Number of jobs that can run concurrently
        .build(task);
    worker.run().await.unwrap();
}
