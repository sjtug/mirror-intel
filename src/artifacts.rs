use crate::common::{Task, MAX_CONCURRENT_DOWNLOAD};
use crate::error::{Error, Result};
use crate::storage::stream_to_s3;

use futures_util::StreamExt;
use reqwest::Client;
use rocket::http::hyper::Bytes;

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use tokio::sync::Semaphore;

use slog::{info, o, warn};

fn transform_stream(
    stream: impl futures::Stream<Item = reqwest::Result<Bytes>>,
    logger: slog::Logger,
) -> impl futures::Stream<Item = std::result::Result<Bytes, std::io::Error>> {
    stream.map(move |x| {
        x.map_err(|err| {
            warn!(logger, "failed to receive data: {:?}", err);
            std::io::Error::new(std::io::ErrorKind::Other, err)
        })
    })
}

async fn process_task(task: Task, client: Client, logger: slog::Logger) -> Result<()> {
    let (content_length, stream) =
        stream_from_url(client, task.origin.clone(), logger.clone()).await?;
    info!(logger, "get {}, length={}", task.path, content_length);
    let key = format!("{}/{}", task.storage, task.path);
    stream_to_s3(&key, content_length, rusoto_s3::StreamingBody::new(stream)).await?;
    info!(logger, "upload {} {} to bucket", task.storage, task.path);
    Ok(())
}

async fn stream_from_url(
    client: Client,
    url: String,
    logger: slog::Logger,
) -> Result<(
    u64,
    Pin<
        Box<
            dyn futures::stream::Stream<Item = std::result::Result<Bytes, std::io::Error>>
                + Sync
                + Send,
        >,
    >,
)> {
    let response = client.get(&url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    if let Some(content_length) = response.content_length() {
        Ok((
            content_length,
            Pin::from(Box::new(transform_stream(response.bytes_stream(), logger))),
        ))
    } else {
        let resp = response.bytes().await?;
        Ok((
            resp.len() as u64,
            Pin::from(Box::new(futures::stream::iter(vec![resp]).map(|x| Ok(x)))),
        ))
    }
}

pub async fn download_artifacts(mut rx: Receiver<Task>, client: Client, logger: slog::Logger) {
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOAD));
    while let Some(task) = rx.recv().await {
        let logger = logger.new(o!("storage" => task.storage.clone(), "origin" => task.origin.clone(), "path" => task.path.clone()));
        info!(logger, "start download");
        let permit = Arc::clone(&sem).acquire_owned().await;
        let client = client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = process_task(task, client, logger.clone()).await {
                warn!(logger, "{:?}", err);
            }
        });
    }
}
