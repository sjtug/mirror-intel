//! Artifact download implementation.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use reqwest::Client;
use rocket::http::hyper::Bytes;
use slog::{debug, info, o, warn};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc::{unbounded_channel, Receiver};
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio_util::codec;

use crate::common::{Config, Metrics, Task};
use crate::error::{Error, Result};
use crate::storage::stream_to_s3;

type IOResult = std::result::Result<Bytes, std::io::Error>;

// So if we make `Logger` global, we can convert this into a `transpose` on an adhoc trait.
fn transform_stream(
    stream: impl Stream<Item = reqwest::Result<Bytes>>,
    logger: slog::Logger,
) -> impl Stream<Item = IOResult> {
    stream.map(move |x| {
        x.map_err(|err| {
            warn!(logger, "failed to receive data: {:?}", err);
            std::io::Error::new(std::io::ErrorKind::Other, err)
        })
    })
}

/// Global unique file id counter.
static FILE_ID: AtomicUsize = AtomicUsize::new(0);

/// An async file wrapper that can be used as a file-backed stream buffer.
struct FileWrapper {
    path: PathBuf,
    f: Option<BufWriter<File>>,
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
    /// Create a new file at the given path.
    ///
    /// Existing files are truncated.
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

    /// Convert this file into a stream of bytes.
    async fn into_bytes_stream(
        mut self,
        logger: slog::Logger,
    ) -> Result<impl Stream<Item = IOResult>> {
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

/// Convert a stream of bytes to a file-backed one.
///
/// The old stream is consumed and a new stream with the same contents is returned.
/// It can be used to download large files from the network.
async fn to_file_stream(
    mut stream: impl Stream<Item = IOResult> + Unpin,
    logger: slog::Logger,
) -> Result<impl Stream<Item = IOResult>> {
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

/// Convert a stream of bytes to a memory-backed one.
///
/// The old stream is consumed and a new stream with the same contents is returned.
/// It can be used to download small files from the network.
async fn to_memory_stream(
    content_length: usize,
    mut stream: impl Stream<Item = IOResult> + Unpin,
) -> Result<impl Stream<Item = IOResult>> {
    let mut result = Vec::with_capacity(content_length);
    while let Some(v) = stream.next().await {
        let v = v?;
        result.extend_from_slice(&v);
    }
    Ok(futures::stream::iter(vec![Ok(Bytes::from(result))]))
}

/// Cache a task.
///
/// This function does the actual caching part.
/// It's called in `download_artifact`, which does something like concurrency control and retries.
async fn process_task(
    task: Task,
    client: Client,
    config: &Config,
    logger: slog::Logger,
) -> Result<()> {
    if client
        .head(&task.cached(config))
        .send()
        .await?
        .status()
        .is_success()
    {
        info!(logger, "already exists");
        return Ok(());
    }
    let (content_length, stream) =
        stream_from_url(client, task.upstream(), config, logger.clone()).await?;
    info!(logger, "get length={}", content_length);
    let key = task.s3_key()?;
    let result = stream_to_s3(
        &key,
        content_length,
        rusoto_s3::StreamingBody::new(stream),
        &config.s3.bucket,
    )
    .await?;
    info!(logger, "upload to bucket");
    debug!(logger, "{:?}", result);
    Ok(())
}

/// Download a stream of bytes from the given url.
async fn stream_from_url(
    client: Client,
    url: String,
    config: &Config,
    logger: slog::Logger,
) -> Result<(u64, Pin<Box<dyn Stream<Item = IOResult> + Sync + Send>>)> {
    let response = client.get(&url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    // TODO ehhh I believe this whole thing can be extracted into an adhoc trait, maybe called ".downloaded()"?
    if let Some(content_length) = response.content_length() {
        if content_length > config.ignore_threshold_mb * 1024 * 1024 {
            return Err(Error::TooLarge(()));
        } else if content_length > config.file_threshold_mb * 1024 * 1024 {
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

/// Main artifact download task.
///
/// This function handles concurrency control, queueing, and retries.
pub async fn download_artifacts(
    mut rx: Receiver<Task>,
    client: Client,
    logger: slog::Logger,
    config: &Config,    // TODO what about pass in an Arc<Config>, or we generate an Arc<Config> at the first place?
    metrics: Arc<Metrics>,
) {
    let sem = Arc::new(Semaphore::new(config.concurrent_download));
    let processing_task = Arc::new(Mutex::new(HashSet::<String>::new()));
    let (fail_tx, mut fail_rx) = unbounded_channel();
    let config = Arc::new(config.clone());

    // TODO convert to while let Some(_)
    loop {
        // Breaks when either of the channels is closed?
        // Should not have much difference with
        // `while let Some(_) = select!(fail_rx.recv(), rx.recv())` if nothing goes wrong,
        // but it's not clear what happens if a task panics.
        // Need to ensure the program terminates when this happens for a quick recovery.
        let mut task: Task;
        tokio::select! {
            val = fail_rx.recv() => { if let Some(val) = val { task = val; } else { break; } }
            val = rx.recv() => { if let Some(val) = val { task = val; } else { break; } }
        }

        // Apply override rules on the task.
        task.to_download_task(&config.endpoints.overrides);

        metrics.task_in_queue.dec();

        // We need to ensure that the total count of pending tasks doesn't exceed the set limit.
        // The income `rx` is already bounded by `max_pending_task`, so it's the retried tasks that
        // are the problem.
        // If a task is retried and current pending queue is full, this will randomly ignore a
        // retried task or an incoming task.

        // TODO I don't think the current double queue design is good. We need to prio income over
        // retries, i.e. income overtakes retries.
        if metrics.task_in_queue.get() > config.max_pending_task as i64 {
            continue;
        }

        // TODO Oh I see why making logger global is blocked. What about replace slog with tracing?
        let logger = logger.new(o!("storage" => task.storage.clone(), "origin" => task.origin.clone(), "path" => task.path.clone()));

        if task.ttl == 0 {
            // The task has been retries too many times. Skip it.
            continue;
        }

        let task_hash = task.upstream();

        {
            // Deduplicate tasks.
            let mut processing_task = processing_task.lock().await;
            if processing_task.contains(&task_hash) {
                info!(logger, "already processing, continue to next task");
                continue;
            }
            processing_task.insert(task_hash.clone());
        }

        info!(logger, "start download");
        metrics.download_counter.inc();

        // Wait for concurrency permit.
        let permit = Arc::clone(&sem).acquire_owned().await;

        let client = client.clone();
        let processing_task = processing_task.clone();
        let metrics = metrics.clone();
        let fail_tx = fail_tx.clone();
        let config = config.clone();

        metrics.task_download.inc();
        // Spawn actual task download task.
        tokio::spawn(async move {
            let _permit = permit;
            let mut task_new = task.clone();

            info!(logger, "begin stream");
            let task_fut = process_task(task, client, &config, logger.clone());
            let task_fut = tokio::time::timeout(
                std::time::Duration::from_secs(config.download_timeout),
                task_fut,
            );
            if let Err(err) = task_fut.await.unwrap_or_else(|_| Err(Error::Timeout(()))) {
                warn!(logger, "{:?}, ttl={}", err, task_new.ttl);
                task_new.ttl -= 1;
                metrics.failed_download_counter.inc();

                {
                    let mut processing_task = processing_task.lock().await;
                    processing_task.remove(&task_hash);
                }

                if !matches!(err, Error::HTTPError(_)) && !matches!(err, Error::TooLarge(_)) {
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
    use slog::Drain;
    use tempdir::TempDir;

    use super::*;

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
