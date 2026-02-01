use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;
use email_service::Email;
use tracing::{Instrument, Span};
use tracing_subscriber::prelude::*;

trait ServiceHandler {
    fn handle(self) -> impl Future<Output = Result<()>> + Send;
}

impl ServiceHandler for Email {
    async fn handle(self) -> Result<()> {
        Ok(())
    }
}

impl ServiceHandler for String {
    async fn handle(self) -> Result<()> {
        Ok(())
    }
}

async fn service<T: ServiceHandler>(input: T) -> Result<()> {
    tracing::info!("Calling Handler");
    let fut = input.handle().instrument(Span::current());
    fut.await
}

async fn run_worker<T>() -> Result<()>
where
    T: ServiceHandler + Send + Unpin + Clone + 'static,
{
    let backend: MemoryStorage<T> = MemoryStorage::new();

    let name = format!("worker-{}", std::any::type_name::<T>());
    let worker = WorkerBuilder::new(name)
        .backend(backend)
        .enable_tracing()
        .build(service::<T>)
        .run();
    Ok(worker.await?)
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

    let email_worker = run_worker::<Email>();
    let string_worker = run_worker::<String>();

    tokio::try_join!(email_worker, string_worker)?;

    Ok(())
}
