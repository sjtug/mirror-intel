use crate::common::{Config, Metrics, Task};
use crate::error::{Error, Result};
use crate::storage::stream_to_s3;

use futures_util::StreamExt;
use reqwest::Client;
use rocket::http::hyper::Bytes;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, TryStreamExt};
use std::sync::atomic::AtomicUsize;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc::{unbounded_channel, Receiver};
use tokio::sync::Mutex;
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
    pub f: Option<BufWriter<File>>,
}

impl AsMut<BufWriter<File>> for FileWrapper {
    fn as_mut(&mut self) -> &mut BufWriter<File> {
        self.f.as_mut().unwrap()
    }
}

impl AsRef<BufWriter<File>> for FileWrapper {
    fn as_ref(&self) -> &BufWriter<File> {
        self.f.as_ref().unwrap()
    }
}

impl FileWrapper {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            f: Some(BufWriter::new(
                OpenOptions::default()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .read(true)
                    .open(path)
                    .await?,
            )),
        })
    }

    async fn into_bytes_stream(
        mut self,
        logger: slog::Logger,
    ) -> Result<impl Stream<Item = IoResult>> {
        // remove file on disk, but we could still read it
        let mut f = self.f.take().unwrap();
        f.flush().await?;
        let mut f = f.into_inner();
        if let Err(err) = fs::remove_file(&self.path).await {
            warn!(
                logger,
                "failed to remove cache file: {:?} {:?}", err, self.path
            );
        }
        f.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(
            codec::FramedRead::new(BufReader::new(f), codec::BytesCodec::new())
                .map_ok(|bytes| bytes.freeze()),
        )
    }
}

impl Drop for FileWrapper {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            drop(f);
            std::fs::remove_file(&self.path).ok();
        }
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
        file.as_mut().write_all(&v).await?;
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
    info!(logger, "get length={}", content_length);
    let key = format!("{}/{}", task.storage, task.path);
    stream_to_s3(&key, content_length, rusoto_s3::StreamingBody::new(stream)).await?;
    info!(logger, "upload to bucket");
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

pub async fn download_artifacts(
    mut rx: Receiver<Task>,
    client: Client,
    logger: slog::Logger,
    config: &Config,
    metrics: Arc<Metrics>,
) {
    let sem = Arc::new(Semaphore::new(config.concurrent_download));
    let processing_task = Arc::new(Mutex::new(HashSet::<String>::new()));
    let (fail_tx, mut fail_rx) = unbounded_channel();

    loop {
        let task: Task;
        tokio::select! {
            val = fail_rx.next() => { if let Some(val) = val { task = val; } else { break; } }
            val = rx.next() => { if let Some(val) = val { task = val; } else { break; } }
        }

        metrics.task_in_queue.dec();

        let logger = logger.new(o!("storage" => task.storage.clone(), "origin" => task.origin.clone(), "path" => task.path.clone()));

        if task.ttl == 0 {
            continue;
        }

        let task_hash = format!("{}/{}", task.origin, task.path);

        {
            let mut processing_task = processing_task.lock().await;
            if processing_task.contains(&task_hash) {
                info!(logger, "already processing, continue to next task");
                continue;
            }
            processing_task.insert(task_hash.clone());
        }

        info!(logger, "start download");
        metrics.download_counter.inc();

        let permit = Arc::clone(&sem).acquire_owned().await;
        let client = client.clone();
        let processing_task = processing_task.clone();
        let metrics = metrics.clone();
        let fail_tx = fail_tx.clone();

        metrics.task_download.inc();
        tokio::spawn(async move {
            let _permit = permit;
            let mut task_new = task.clone();

            info!(logger, "begin stream");
            if let Err(err) = process_task(task, client, logger.clone()).await {
                warn!(logger, "{:?}, ttl={}", err, task_new.ttl);
                task_new.ttl -= 1;
                metrics.failed_download_counter.inc();

                {
                    let mut processing_task = processing_task.lock().await;
                    processing_task.remove(&task_hash);
                }

                if !matches!(err, Error::HTTPError(_)) {
                    fail_tx.send(task_new).unwrap();
                    metrics.task_in_queue.inc();
                }
            } else {
                let mut processing_task = processing_task.lock().await;
                processing_task.remove(&task_hash);
            }

            metrics.task_download.dec();
        });
    }

    info!(logger, "artifact download stop");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    use slog::Drain;

    fn create_test_logger() -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    #[tokio::test]
    async fn test_overlay_file_create() {
        let logger = create_test_logger();
        let tmp_dir = TempDir::new("intel").unwrap();
        let path = tmp_dir.path().join("test.bin");
        let mut wrapper = FileWrapper::open(&path).await.unwrap();
        wrapper.as_mut().write_all(b"233333333").await.unwrap();
        let mut stream = wrapper.into_bytes_stream(logger).await.unwrap();
        assert_eq!(&stream.next().await.unwrap().unwrap(), "233333333");
    }
}
