use apalis::{config::WorkerConfig, config::WorkerFromConfig, prelude::*};
use apalis_file_storage::JsonStorage;

const CONFIG: &str = r#"
{
  "name": "simple-worker",
  "backend": "/tmp/test.json",
  "middleware": [
    "CatchPanic",
    "Tracing"
  ]
}
"#;

type Config = WorkerConfig<JsonStorage<u32>>;

async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(task, 42);
    worker.stop()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let config: Config = serde_json::from_str(CONFIG).unwrap();

    let worker = WorkerBuilder::try_config(config)?
        .map_backend(|backend| {
            backend.after_start(move |b| {
                let mut b = b.clone();
                async move {
                    b.push(42).await.unwrap();
                    Ok(())
                }
            })
        })
        .build(task);

    worker.run().await?;

    Ok(())
}
