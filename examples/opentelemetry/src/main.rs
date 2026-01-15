//! Run with
//!
//! ```not_rust
//! cd examples && cargo run -p opentelemetry-example
//! ```

// Based on https://github.com/open-telemetry/opentelemetry-rust/blob/f76f68a7eaa95227ad8fd9554f171faaa4444ccf/opentelemetry-prometheus/examples/hyper.rs

use anyhow::Result;
use apalis::layers::opentelemetry::OpenTelemetryMetricsLayer;
use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use axum::{
    extract::{Form, State},
    http::{header::CONTENT_TYPE, StatusCode},
    response::{Html, IntoResponse},
    routing::get,
    Extension, Router,
};
use opentelemetry::global;
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};
use prometheus::{Encoder, Registry, TextEncoder};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, net::SocketAddr, sync::OnceLock};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
// use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};

use email_service::{send_email, Email, FORM_HTML};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let backend = JsonStorage::new_temp().unwrap();

    let (meter_provider, registry) = setup_metrics_provider();
    global::set_meter_provider(meter_provider);

    // build our application with some routes
    let app = Router::new()
        .route("/", get(show_form).post(add_new_job::<Email>))
        .layer(Extension(backend.clone()))
        .route(
            "/metrics",
            get(async |State(registry): State<Registry>| {
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                encoder
                    .encode(&metric_families, &mut buffer)
                    .expect("Could not encode");

                ([(CONTENT_TYPE, encoder.format_type().to_string())], buffer)
            }),
        )
        .with_state(registry);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::debug!("listening on {}", addr);

    let http = async {
        axum::serve(listener, app)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
    };
    let worker = async {
        WorkerBuilder::new("tasty-banana")
            .backend(backend.clone())
            .layer(OpenTelemetryMetricsLayer::default())
            .build(send_email)
            .run()
            .await
            .expect("Worker failed");
        Ok(())
    };
    let _res = futures::future::try_join(worker, http)
        .await
        .expect("Could not start services");
    Ok(())
}

fn get_resource() -> Resource {
    static RESOURCE: OnceLock<Resource> = OnceLock::new();
    RESOURCE.get_or_init(|| Resource::builder().build()).clone()
}

fn setup_metrics_provider() -> (SdkMeterProvider, Registry) {
    // With opentelemetry collector running:
    // let otlp_exporter = MetricExporter::builder()
    //     .with_http()
    //     .with_protocol(Protocol::HttpBinary)
    //     .build()
    //     .expect("Could not create OTLP metric exporter");

    let registry = Registry::new();

    let prom_exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("Could not create prometheus metric exporter");

    let meter_provider = SdkMeterProvider::builder()
        // .with_periodic_exporter(otlp_exporter)
        .with_reader(prom_exporter)
        .with_resource(get_resource())
        .build();

    (meter_provider, registry)
}

async fn show_form() -> Html<&'static str> {
    Html(FORM_HTML)
}

async fn add_new_job<T>(
    Extension(mut storage): Extension<JsonStorage<T>>,
    Form(input): Form<T>,
) -> impl IntoResponse
where
    T: 'static + Debug + Serialize + DeserializeOwned + Unpin + Send + Sync,
{
    dbg!(&input);
    let new_job = storage.push(input).await;

    match new_job {
        Ok(ctx) => (
            StatusCode::CREATED,
            format!("Job [{ctx:?}] was successfully added"),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("An Error occurred {e}"),
        ),
    }
}
