use anyhow::Result;
use apalis::prelude::*;
use futures::TryStreamExt;
use image::GenericImageView;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessedImage {
    thumbnail: Vec<u8>,
    metadata: ImageMetadata,
    moderation_status: ModerationResult,
    user_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImageMetadata {
    width: u32,
    height: u32,
    format: String,
    exif: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModerationResult {
    is_safe: bool,
    confidence: f32,
    flags: Vec<String>,
}

async fn download_image(url: &str) -> Result<Vec<u8>> {
    let response = reqwest::get(url).await?;
    let bytes = response.bytes().await?;
    Ok(bytes.to_vec())
}

async fn generate_thumbnail(image_data: Vec<u8>, width: u32, height: u32) -> Result<Vec<u8>> {
    let img = image::load_from_memory(&image_data)?;
    let thumbnail = img.resize(width, height, image::imageops::FilterType::Lanczos3);

    let mut buffer = Vec::new();
    thumbnail.write_to(
        &mut std::io::Cursor::new(&mut buffer),
        image::ImageFormat::Jpeg,
    )?;
    Ok(buffer)
}

async fn extract_exif_data(image_data: Vec<u8>) -> Result<ImageMetadata> {
    let img = image::load_from_memory(&image_data)?;
    let (width, height) = img.dimensions();

    let mut exif = HashMap::new();
    exif.insert("camera".to_string(), "Unknown".to_string());

    Ok(ImageMetadata {
        width,
        height,
        format: "JPEG".to_string(),
        exif,
    })
}

async fn check_content_safety(_url: &str) -> Result<ModerationResult> {
    // Simulate API call to content moderation service
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    Ok(ModerationResult {
        is_safe: true,
        confidence: 0.95,
        flags: vec![],
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ImageProcessingTask {
    image_url: String,
    user_id: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum ProcessingResult {
    Thumb(Vec<u8>),
    Meta(ImageMetadata),
    Moderation(ModerationResult),
}

async fn process_image(task: ImageProcessingTask, runner: Runner) -> Result<ProcessedImage> {
    let (ctx, receiver) = runner.channel::<Result<ProcessingResult>>();

    tokio::spawn(ctx.execute({
        let url = task.image_url.clone();
        async move {
            let img = download_image(&url).await?;
            let thumb = generate_thumbnail(img, 200, 200).await?;
            Ok(ProcessingResult::Thumb(thumb))
        }
    }));

    tokio::spawn(ctx.execute({
        let url = task.image_url.clone();
        async move {
            let img = download_image(&url).await?;
            let meta = extract_exif_data(img).await?;
            Ok(ProcessingResult::Meta(meta))
        }
    }));

    tokio::spawn(ctx.execute({
        let url = task.image_url.clone();
        async move {
            let moderation = check_content_safety(&url).await?;
            Ok(ProcessingResult::Moderation(moderation))
        }
    }));

    ctx.wait().await;
    let results: Vec<std::result::Result<ProcessingResult, _>> =
        receiver.try_collect::<Vec<_>>().await?;

    let mut thumbnail = None;
    let mut metadata = None;
    let mut moderation_status = None;

    for result in results {
        match result? {
            ProcessingResult::Thumb(t) => thumbnail = Some(t),
            ProcessingResult::Meta(m) => metadata = Some(m),
            ProcessingResult::Moderation(mod_result) => moderation_status = Some(mod_result),
        }
    }

    Ok(ProcessedImage {
        thumbnail: thumbnail.ok_or_else(|| anyhow::anyhow!("Missing thumbnail"))?,
        metadata: metadata.ok_or_else(|| anyhow::anyhow!("Missing metadata"))?,
        moderation_status: moderation_status
            .ok_or_else(|| anyhow::anyhow!("Missing moderation"))?,
        user_id: task.user_id,
    })
}

async fn produce_task(storage: &mut MemoryStorage<ImageProcessingTask>) {
    let image_url =
        "https://images.pexels.com/photos/10970368/pexels-photo-10970368.jpeg".to_owned();
    storage
        .push(ImageProcessingTask {
            user_id: 42,
            image_url,
        })
        .await
        .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    unsafe {
        std::env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();
    let mut backend = MemoryStorage::new();
    produce_task(&mut backend).await;

    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .enable_tracing()
        .concurrency(1)
        .long_running()
        .on_event(|_c, e| info!("{e}"))
        .build(process_image)
        .run_until(tokio::signal::ctrl_c()) // Graceful shutdown on Ctrl+C
        .await?;
    Ok(())
}
