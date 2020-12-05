use crate::common::{Task, MAX_CONCURRENT_DOWNLOAD};
use crate::error::{Error, Result};
use crate::storage::stream_to_s3;

use futures_util::StreamExt;
use reqwest::Client;
use rocket::http::hyper::Bytes;

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, TryStreamExt};
use std::sync::atomic::AtomicUsize;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Semaphore;
use tokio_util::codec;

use slog::{info, o, warn};

type IoResult = std::result::Result<Bytes, std::io::Error>;

fn transform_stream(
    stream: impl Stream<Item = reqwest::Result<Bytes>>,
    logger: slog::Logger,
) -> impl Stream<Item = IoResult> {
    stream.map(move |x| {
        x.map_err(|err| {
            warn!(logger, "failed to receive data: {:?}", err);
            std::io::Error::new(std::io::ErrorKind::Other, err)
        })
    })
}

static FILE_ID: AtomicUsize = AtomicUsize::new(0);

struct FileWrapper {
    path: PathBuf,
    pub f: File,
}

impl FileWrapper {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            f: OpenOptions::default()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(path)
                .await?,
        })
    }

    async fn into_bytes_stream(
        mut self,
        logger: slog::Logger,
    ) -> Result<impl Stream<Item = IoResult>> {
        // remove file on disk, but we could still read it
        if let Err(err) = fs::remove_file(&self.path).await {
            warn!(
                logger,
                "failed to remove cache file: {:?} {:?}", err, self.path
            );
        }
        self.f.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(codec::FramedRead::new(self.f, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze()))
    }
}

async fn to_file_stream(
    mut stream: impl Stream<Item = IoResult> + Unpin,
    logger: slog::Logger,
) -> Result<impl Stream<Item = IoResult>> {
    let path = format!(
        "/mnt/cache/{}",
        FILE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    );
    let mut file = FileWrapper::open(&path).await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        file.f.write_all(&v).await?;
    }
    Ok(file.into_bytes_stream(logger).await?)
}

async fn to_memory_stream(
    content_length: usize,
    mut stream: impl Stream<Item = IoResult> + Unpin,
) -> Result<impl Stream<Item = IoResult>> {
    let mut result = Vec::with_capacity(content_length);
    while let Some(v) = stream.next().await {
        let v = v?;
        result.extend_from_slice(&v);
    }
    Ok(futures::stream::iter(vec![Ok(Bytes::from(result))]))
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
) -> Result<(u64, Pin<Box<dyn Stream<Item = IoResult> + Sync + Send>>)> {
    let response = client.get(&url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    if let Some(content_length) = response.content_length() {
        if content_length > 40 * 1024 * 1024 {
            info!(logger, "stream mode: file backend");
            let stream = transform_stream(response.bytes_stream(), logger.clone());
            let stream = to_file_stream(stream, logger).await?;
            Ok((content_length, Box::pin(stream)))
        } else if content_length > 1024 * 1024 {
            info!(logger, "stream mode: memory cache");
            let stream = transform_stream(response.bytes_stream(), logger);
            let stream = to_memory_stream(content_length as usize, stream).await?;
            Ok((content_length, Box::pin(stream)))
        } else {
            info!(logger, "stream mode: direct copy");
            let stream = transform_stream(response.bytes_stream(), logger);
            Ok((content_length, Box::pin(stream)))
        }
    } else {
        info!(logger, "stream mode: direct copy");
        let resp = response.bytes().await?;
        Ok((
            resp.len() as u64,
            Box::pin(futures::stream::iter(vec![resp]).map(|x| Ok(x))),
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
