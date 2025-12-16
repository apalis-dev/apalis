use std::time::Duration;

use apalis::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LongRunningJob {}

async fn long_running_task(_task: LongRunningJob, worker: WorkerContext) {
    loop {
        info!("is_shutting_down: {}", worker.is_shutting_down());
        tokio::time::sleep(Duration::from_secs(5)).await; // Do some hard thing
        info!("Long running task heartbeat");
        if worker.is_shutting_down() {
            info!("saving the job state");
            tokio::time::sleep(Duration::from_secs(5)).await; // Simulate saving state
            break;
        }
    }
    info!("Shutdown complete!");
}

async fn produce_jobs(storage: &mut MemoryStorage<LongRunningJob>) {
    storage.push(LongRunningJob {}).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    tracing_subscriber::fmt::init();
    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await;
    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .enable_tracing()
        .concurrency(2)
        .on_event(|_c, e| info!("{e}"))
        .build(long_running_task)
        .run_until(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}
