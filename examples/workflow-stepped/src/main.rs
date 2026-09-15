use std::{fmt::Debug, time::Duration};

use apalis::prelude::*;
use apalis_workflow::{SteppedFlow, in_memory::InMemoryWorkflow};
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Serialize, Deserialize)]
struct WelcomeEmail {
    user_id: usize,
}

#[derive(Debug, Serialize, Deserialize)]

struct CampaignEmail {
    campaign_id: usize,
}

#[derive(Debug, Serialize, Deserialize)]

struct CompleteCampaign {
    completion_id: usize,
}

type Store = Shared<InMemoryWorkflow<WelcomeEmail>>;

async fn welcome(req: WelcomeEmail, _data: Data<Store>) -> Result<CampaignEmail, BoxDynError> {
    Ok::<_, _>(CampaignEmail {
        campaign_id: req.user_id + 1,
    })
}

async fn campaign(req: CampaignEmail, _data: Data<Store>) -> Result<CompleteCampaign, BoxDynError> {
    Ok::<_, _>(CompleteCampaign {
        completion_id: req.campaign_id + 1,
    })
}

async fn complete_campaign(
    _req: CompleteCampaign,
    _data: Data<Store>,
    wrk: WorkerContext,
) -> Result<String, BoxDynError> {
    wrk.stop()?;
    Ok::<_, _>("Completed job successfully".to_owned())
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

    let mut backend = InMemoryWorkflow::create();

    backend.push(WelcomeEmail { user_id: 1 }).await.unwrap();

    let workflow = SteppedFlow::new("example-campaign")
        .and_then(welcome)
        .delay_for(Duration::from_secs(1))
        .and_then(campaign)
        .and_then(complete_campaign);

    WorkerBuilder::new("tasty-banana")
        .backend(backend.clone())
        .data(backend)
        .parallelize(tokio::spawn)
        .enable_tracing()
        .concurrency(2)
        .on_event(|_c, e| info!("{e}"))
        .build(workflow)
        .run()
        .await?;
    Ok(())
}
