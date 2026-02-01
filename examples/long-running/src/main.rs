use anyhow::Result;
use apalis::prelude::*;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DataExportTask {
    user_id: i32,
    start_date: String,
    end_date: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum ExportResult {
    Orders(Vec<String>),
    Analytics(String),
    UserData(String),
}

async fn export_orders(user_id: i32, start: &str, end: &str) -> Result<Vec<String>> {
    tokio::time::sleep(Duration::from_secs(90)).await;
    Ok(vec![format!(
        "Order data for user {user_id} from {start} to {end}"
    )])
}

async fn generate_analytics(user_id: i32, start: &str, end: &str) -> Result<String> {
    tokio::time::sleep(Duration::from_secs(120)).await;
    Ok(format!(
        "Analytics for user {user_id} from {start} to {end}"
    ))
}

async fn export_user_data(user_id: i32) -> Result<String> {
    tokio::time::sleep(Duration::from_secs(80)).await;
    Ok(format!("User data export for {user_id}"))
}

async fn process_export(task: DataExportTask, runner: Runner) -> Result<String> {
    let (ctx, receiver) = runner.channel::<Result<ExportResult>>();

    tokio::spawn(ctx.execute({
        let start = task.start_date.clone();
        let end = task.end_date.clone();
        async move {
            let orders = export_orders(task.user_id, &start, &end).await?;
            Ok(ExportResult::Orders(orders))
        }
    }));

    tokio::spawn(ctx.execute({
        let start = task.start_date.clone();
        let end = task.end_date.clone();
        async move {
            let analytics = generate_analytics(task.user_id, &start, &end).await?;
            Ok(ExportResult::Analytics(analytics))
        }
    }));

    tokio::spawn(ctx.execute({
        async move {
            let user_data = export_user_data(task.user_id).await?;
            Ok(ExportResult::UserData(user_data))
        }
    }));

    ctx.wait().await;
    let results = receiver.try_collect::<Vec<_>>().await?;

    let mut orders = None;
    let mut analytics = None;
    let mut user_data = None;

    for result in results {
        match result? {
            ExportResult::Orders(o) => orders = Some(o),
            ExportResult::Analytics(a) => analytics = Some(a),
            ExportResult::UserData(u) => user_data = Some(u),
        }
    }

    Ok(format!(
        "Export complete: {} orders, analytics: {}, user_data: {}",
        orders
            .ok_or_else(|| anyhow::anyhow!("Missing orders"))?
            .len(),
        analytics.ok_or_else(|| anyhow::anyhow!("Missing analytics"))?,
        user_data.ok_or_else(|| anyhow::anyhow!("Missing user data"))?
    ))
}

async fn produce_task(storage: &mut MemoryStorage<DataExportTask>) {
    storage
        .push(DataExportTask {
            user_id: 42,
            start_date: "2024-01-01".to_string(),
            end_date: "2024-12-31".to_string(),
        })
        .await
        .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    tracing_subscriber::fmt::init();
    let mut backend = MemoryStorage::new();
    produce_task(&mut backend).await;

    WorkerBuilder::new("export-worker")
        .backend(backend)
        .enable_tracing()
        .concurrency(1)
        .long_running()
        .on_event(|_c, e| info!("{e}"))
        .build(process_export)
        .run_until(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}
