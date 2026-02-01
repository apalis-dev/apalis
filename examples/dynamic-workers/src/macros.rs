use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::prelude::*;

macro_rules! register_workers {
    ($monitor:expr, [
        $(
            $worker_name:literal => {
                backend: $backend:expr,
                handler: $handler:expr
                $(, concurrency: $concurrency:expr)?
                $(, rate_limit: ($rate_count:expr, $rate_duration:expr))?
            }
        ),* $(,)?
    ]) => {{
        let monitor = $monitor;
        $(
            let monitor = {
                monitor.register(move |_runs| {
                    let builder = WorkerBuilder::new($worker_name)
                        .backend($backend)
                        .catch_panic()
                        .enable_tracing();

                    $(
                        let builder = builder.concurrency($concurrency);
                    )?

                    $(
                        let builder = builder.rate_limit($rate_count, $rate_duration);
                    )?

                    builder.build($handler)
                })
            };
        )*
        monitor
    }};
}

async fn handler_1(_: String) {}
async fn handler_2(_: usize) {}
async fn handler_3(_: u32) {}
async fn handler_4(_: u64) {}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let monitor = register_workers!(
        Monitor::new(),
        [
            "handler_1" => {
                backend: JsonStorage::new_temp().unwrap(),
                handler: handler_1,
                concurrency: 1
            },
            "handler_2" => {
                backend: MemoryStorage::new(),
                handler: handler_2
            },
            "handler_3" => {
                backend: JsonStorage::new_temp().unwrap(),
                handler: handler_3,
                rate_limit: (10, Duration::new(5, 0))
            },
            "handler_4" => {
                backend: MemoryStorage::new(),
                handler: handler_4,
                rate_limit: (40, Duration::new(5, 0))
            },
        ]
    );
    monitor
        // Collect all the events from all workers
        .on_event(|ctx, ev| {
            info!("Received {} event from {} Worker", ev, ctx.name());
        })
        // Define when a worker should restart
        .should_restart(|ctx, err, runs| {
            if ctx.name() == "tasty-pear"
                && err.to_string().contains("Recoverable Error")
                && runs < 5
            {
                return true;
            }
            false
        })
        // Graceful shutdown will wait 5s before forcing an exit if workers have not stopped
        .shutdown_timeout(Duration::from_secs(5))
        // Shutdown will be triggered by CTRL + C
        .run_with_signal(tokio::signal::ctrl_c())
        .await?;

    Ok(())
}
