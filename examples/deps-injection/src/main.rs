#![allow(missing_docs)]
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use apalis::prelude::*;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug)]
pub struct ApiClient;

impl<T: Sync> FromRequest<T> for ApiClient {
    type Error = io::Error;
    async fn from_request(_req: &T) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

// A task can have up to 16 arguments that implement `FromRequest`
async fn simple_job(
    args: i32, // Required, must be of the type of the job/message and the first argument
    task: TaskContext, // The current execution context
    worker: WorkerContext, // The worker and its context, added by worker
    task_id: TaskId, // The task id, provided by backend
    attempt: Attempt, // The current attempt
    count: Data<Counter>, // Our custom data added via layer
    client: ApiClient, // Injected via `FromRequest`
) {
    // increment the counter
    let current = count.fetch_add(1, Ordering::Relaxed);
    info!(
        "worker: {worker:?}; task_id: {task_id:?}, attempt:{attempt:?} count: {current:?} client: {client:?}"
    );

    info!("task run for: {:?}", task.elapsed());

    if args == 9 {
        worker.stop().unwrap();
    }
}

async fn produce_jobs(storage: &mut MemoryStorage<i32>) {
    for i in 0..10 {
        storage.push(i).await.unwrap();
    }
}

type Counter = Arc<AtomicUsize>;

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
        .data(Counter::default())
        .build(simple_job)
        .run()
        .await?;
    Ok(())
}
