use anyhow::Result;
use apalis::prelude::*;
use futures::{FutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
    tokio::time::sleep(Duration::from_secs(9)).await;
    Ok(vec![format!(
        "Order data for user {user_id} from {start} to {end}"
    )])
}

async fn generate_analytics(user_id: i32, start: &str, end: &str) -> Result<String> {
    tokio::time::sleep(Duration::from_secs(12)).await;
    Ok(format!(
        "Analytics for user {user_id} from {start} to {end}"
    ))
}

async fn export_user_data(user_id: i32) -> Result<String> {
    tokio::time::sleep(Duration::from_secs(8)).await;
    Ok(format!("User data export for {user_id}"))
}

async fn process_export(
    task: DataExportTask,
    mut runner: TaskRunner<Result<ExportResult>>,
) -> Result<String> {
    runner.execute(tokio::spawn({
        let start = task.start_date.clone();
        let end = task.end_date.clone();
        async move {
            let orders = export_orders(task.user_id, &start, &end).await?;
            Ok(ExportResult::Orders(orders))
        }
    }));

    runner.execute(tokio::spawn({
        let start = task.start_date.clone();
        let end = task.end_date.clone();
        async move {
            let analytics = generate_analytics(task.user_id, &start, &end).await?;
            Ok(ExportResult::Analytics(analytics))
        }
    }));

    runner.execute(tokio::spawn({
        async move {
            let user_data = export_user_data(task.user_id).await?;
            Ok(ExportResult::UserData(user_data))
        }
    }));

    let results = runner.try_collect::<Vec<_>>().await?;

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
            start_date: "2024-01-01".to_owned(),
            end_date: "2024-12-31".to_owned(),
        })
        .await
        .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    use tracing_subscriber::EnvFilter;

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let cpu_n = std::thread::available_parallelism().unwrap().get();

    let mut backend = MemoryStorage::new();
    produce_task(&mut backend).await;

    WorkerBuilder::new("export-worker")
        .backend(backend)
        .enable_tracing()
        .concurrency(cpu_n)
        .long_running()
        .on_event(|_c, e| info!("{e}"))
        .build(process_export)
        .run_with_ctx(move |mut ctx| {
            tokio::signal::ctrl_c().map(move |_| {
                ctx.stop().unwrap();
                ctx.kill().unwrap();
                Ok(())
            })
        })
        .await?;
    Ok(())
}
