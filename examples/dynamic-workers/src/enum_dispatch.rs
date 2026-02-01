use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;
use email_service::Email;
use enum_dispatch::enum_dispatch;
use futures::future::BoxFuture;
use tracing_subscriber::prelude::*;

#[enum_dispatch]
trait ServiceHandler {
    fn handle(self) -> BoxFuture<'static, Result<()>>;
}

#[enum_dispatch(ServiceHandler)]
#[derive(Debug, Clone)]
enum TaskHandler {
    Email,
    String,
}

impl ServiceHandler for Email {
    fn handle(self) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl ServiceHandler for String {
    fn handle(self) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

async fn service(input: TaskHandler) -> Result<()> {
    tracing::info!("Calling Handler");
    input.handle().await
}

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

    let backend: MemoryStorage<TaskHandler> = MemoryStorage::new();

    WorkerBuilder::new("enum_worker")
        .backend(backend)
        .enable_tracing()
        .build(service)
        .run()
        .await?;

    Ok(())
}
