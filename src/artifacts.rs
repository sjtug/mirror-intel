use crate::common::{Task, MAX_CONCURRENT_DOWNLOAD};
use crate::error::{Error, Result};
use crate::storage::stream_to_s3;

use futures_util::StreamExt;
use reqwest::Client;
use rocket::http::hyper::Bytes;

use slog::Drain;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Semaphore;

fn transform_stream(
    stream: impl futures::Stream<Item = reqwest::Result<Bytes>>,
) -> impl futures::Stream<Item = std::result::Result<Bytes, std::io::Error>> {
    stream.map(|x| {
        x.map_err(|err| {
            warn!("failed to receive data: {:?}", err);
            std::io::Error::new(std::io::ErrorKind::Other, err)
        })
    })
}

async fn process_task(task: Task, client: Client) -> Result<()> {
    let (content_length, stream) = stream_from_url(client, task.origin.clone()).await?;
    info!("get {}, length={}", task.path, content_length);
    let key = format!("{}/{}", task.storage, task.path);
    stream_to_s3(&key, content_length, rusoto_s3::StreamingBody::new(stream)).await?;
    info!("upload {} {} to bucket", task.storage, task.path);
    Ok(())
}

async fn stream_from_url(
    client: Client,
    url: String,
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
            Pin::from(Box::new(transform_stream(response.bytes_stream()))),
        ))
    } else {
        let resp = response.bytes().await?;
        Ok((
            resp.len() as u64,
            Pin::from(Box::new(futures::stream::iter(vec![resp]).map(|x| Ok(x)))),
        ))
    }
}

pub async fn download_artifacts(mut rx: Receiver<Task>, client: Client) {
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOAD));
    while let Some(task) = rx.recv().await {
        info!("task received {:?}", task);
        let permit = Arc::clone(&sem).acquire_owned().await;
        let client = client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = process_task(task, client).await {
                warn!("{:?}", err);
            }
        });
    }
}
