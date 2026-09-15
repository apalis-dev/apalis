#![allow(missing_docs)]
use std::time::Duration;

use apalis::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MyTask {}

async fn long_running_task(_task: MyTask, worker: WorkerContext) {
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

async fn produce_jobs(storage: &mut MemoryStorage<MyTask>) {
    storage.push(MyTask {}).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    use tracing_subscriber::EnvFilter;

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug"))
        .unwrap();
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await;
    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .enable_tracing()
        .concurrency(2)
        .build(long_running_task)
        .run_until(tokio::signal::ctrl_c())
        .instrument(tracing::span!(tracing::Level::INFO, "tasty-banana"))
        .await?;
    Ok(())
}
