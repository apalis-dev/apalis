#![allow(missing_docs)]
use std::time::Duration;

use anyhow::Result;
use apalis::layers::tracing::*;
use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use email_service::Email;
use rand::RngExt;
use tokio::time::sleep;

use crate::tui::run_tui;

mod tui;
mod view;

async fn email_service(_: Email, task: TaskContext) -> Result<(), BoxDynError> {
    let range = rand::rng().random_range(1000..=20000);
    tokio::spawn(task.run_until_executed(async move {
        sleep(Duration::from_millis(12000)).await;
    }));
    if range > 7000 {
        task.cancel().unwrap();
        return Ok(());
    }
    sleep(Duration::from_millis(range)).await;
    Err(BoxDynError::from("Failed"))
}

async fn produce_task(storage: &mut JsonStorage<Email>) -> Result<()> {
    loop {
        let _ = storage
            .push(Email {
                to: "test@example".to_owned(),
                text: "Test background job from apalis".to_owned(),
                subject: "Welcome Sentry Email".to_owned(),
            })
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn produce_task_with_ctx(storage: &mut JsonStorage<Email>) -> Result<()> {
    loop {
        let email = Email {
            to: "test@example".to_owned(),
            text: "Test background job from apalis".to_owned(),
            subject: "Welcome Sentry Email".to_owned(),
        };
        let context = TracingContext::from(OtelTraceContext::current());
        let task = TaskBuilder::new(email).metadata(&context).build();
        let _ = storage.push_task(task).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let avocado_backend = JsonStorage::new_temp()?;
    let mut av = avocado_backend.clone();
    tokio::spawn(async move {
        produce_task(&mut av).await.unwrap();
    });

    let pear_backend = JsonStorage::new_temp()?;
    let mut pb = pear_backend.clone();

    tokio::spawn(async move {
        produce_task_with_ctx(&mut pb).await.unwrap();
    });

    let monitor = Monitor::new()
        .register(move |_restarts| {
            WorkerBuilder::new("tasty-avocado")
                .backend(avocado_backend.clone())
                .concurrency(30)
                .enable_tracing()
                .build(email_service)
        })
        .register(move |_restarts| {
            WorkerBuilder::new("tasty-pear")
                .backend(pear_backend.clone())
                .concurrency(20)
                .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
                .build(email_service)
        })
        .should_restart(|wrk, err| {
            let runs = wrk.restarts();
            if wrk.name() == "tasty-pear"
                && err.to_string().contains("Recoverable Error")
                && runs < 5
            {
                return false;
            }
            true
        })
        .shutdown_timeout(Duration::from_secs(5));
    let context = monitor.context();
    let res = tokio::task::spawn_blocking(|| run_tui(context));

    tokio::spawn(res);
    monitor.run().await?;

    Ok(())
}
