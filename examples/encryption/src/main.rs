use apalis::prelude::*;
use encryption::{DecryptService, EncryptedJob, EncryptionKey, push_encrypted};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
    body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    name: String,
    quarter: u8,
}

async fn handle_email(email: Email) -> Result<(), BoxDynError> {
    info!(to = %email.to, subject = %email.subject, "decrypted and processing email");
    Ok(())
}

async fn handle_report(report: Report) -> Result<(), BoxDynError> {
    info!(name = %report.name, quarter = report.quarter, "decrypted and processing report");
    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let key = EncryptionKey::generate();

    let mut email_storage: MemoryStorage<EncryptedJob<Email>> = MemoryStorage::new();
    let mut report_storage: MemoryStorage<EncryptedJob<Report>> = MemoryStorage::new();

    push_encrypted(
        &mut email_storage,
        &key,
        Email {
            to: "alice@example.com".to_string(),
            subject: "Encrypted hello".to_string(),
            body: "This payload was encrypted at rest".to_string(),
        },
    )
    .await
    .unwrap();

    push_encrypted(
        &mut report_storage,
        &key,
        Report {
            name: "Q4 Revenue".to_string(),
            quarter: 4,
        },
    )
    .await
    .unwrap();

    info!("pushed encrypted jobs, starting workers...");

    let email_worker = WorkerBuilder::new("email-worker")
        .backend(email_storage)
        .build(DecryptService::new(key.clone(), handle_email));

    let report_worker = WorkerBuilder::new("report-worker")
        .backend(report_storage)
        .build(DecryptService::new(key, handle_report));

    tokio::select! {
        res = email_worker.run() => res.unwrap(),
        res = report_worker.run() => res.unwrap(),
    }
}
