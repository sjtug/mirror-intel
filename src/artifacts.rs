//! Artifact download implementation.

use std::borrow::Cow;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::{Client, Response};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::{Receiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::{info, instrument, warn};
use url::Url;

use crate::common::{Config, Metrics, Task};
use crate::error::{Error, Result};
use crate::storage::stream_to_s3;

const BYTES_PER_MB: u64 = 1024 * 1024;

/// Downloaded upstream payload.
enum UploadPayload {
    Memory(Bytes),
    File { path: PathBuf, content_length: u64 },
}

impl UploadPayload {
    fn content_length(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::File { content_length, .. } => *content_length,
        }
    }
}

/// Global unique file id counter.
static FILE_ID: AtomicUsize = AtomicUsize::new(0);

fn max_download_size(config: &Config) -> u64 {
    config.ignore_threshold_mb.saturating_mul(BYTES_PER_MB)
}

fn file_threshold(config: &Config) -> u64 {
    config.file_threshold_mb.saturating_mul(BYTES_PER_MB)
}

fn next_buffer_path(config: &Config) -> PathBuf {
    config
        .buffer_path
        .join(FILE_ID.fetch_add(1, Ordering::SeqCst).to_string())
}

/// Remove a temporary file, ignoring not-found errors.
async fn remove_buffer_file(path: &Path) {
    if let Err(err) = fs::remove_file(path).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(error=?err, path=%path.display(), "failed to remove cache file");
    }
}

/// Download response body into memory.
async fn download_to_memory(response: Response, max_size: u64) -> Result<UploadPayload> {
    let body = response.bytes().await?;
    if body.len() as u64 > max_size {
        return Err(Error::TooLarge(()));
    }
    Ok(UploadPayload::Memory(body))
}

/// Download response body into a temporary file.
async fn download_to_file(
    response: Response,
    config: &Config,
    max_size: u64,
) -> Result<UploadPayload> {
    let path = next_buffer_path(config);
    let mut file = BufWriter::new(
        OpenOptions::default()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .await?,
    );

    let result = async {
        let mut stream = response.bytes_stream();
        let mut content_length = 0_u64;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|err| {
                warn!(error=?err, "failed to receive data");
                Error::Reqwest(err)
            })?;
            content_length += chunk.len() as u64;
            if content_length > max_size {
                return Err(Error::TooLarge(()));
            }
            file.write_all(&chunk).await?;
        }

        file.flush().await?;
        Ok(content_length)
    }
    .await;

    match result {
        Ok(content_length) => Ok(UploadPayload::File {
            path,
            content_length,
        }),
        Err(err) => {
            remove_buffer_file(&path).await;
            Err(err)
        }
    }
}

/// Download context for a single artifact.
struct DownloadCtx<'a> {
    task: Task,
    config: Cow<'a, Arc<Config>>,
    metrics: Cow<'a, Arc<Metrics>>,
    client: Cow<'a, Client>,
    processing_task: Cow<'a, Arc<Mutex<HashSet<Url>>>>,
    fail_tx: Cow<'a, UnboundedSender<Task>>,
    extra: DownloadStageExtra<'a>,
}

/// Extra context for different stages of the download process.
enum DownloadStageExtra<'a> {
    Pre {
        sem: &'a Arc<Semaphore>,
    },
    On {
        _permit: OwnedSemaphorePermit,
        task_hash: Url,
    },
}

impl<'a> DownloadCtx<'a> {
    /// Do prepare work before actual download.
    ///
    /// This method checks whether there's an ongoing download for the same artifact,
    /// limit the number of concurrent downloads, and skip the download if retry_limit is reached.
    ///
    /// After the prepare work has been done, you must call `spawn()` immediately to start the
    /// download, or further download tasks will be blocked.
    ///
    /// Returns `None` if the download should be skipped.
    #[instrument(skip(self), fields(storage = self.task.storage, origin = self.task.origin, path = self.task.path))]
    pub async fn prepare(self) -> Option<DownloadCtx<'static>> {
        self.metrics.task_in_queue.dec();

        // We need to ensure that the total count of pending tasks doesn't exceed the set limit.
        // The income `rx` is already bounded by `max_pending_task`, so it's the retried tasks that
        // are the problem.
        // If a task is retried and current pending queue is full, this will randomly ignore a
        // retried task or an incoming task.

        // TODO I don't think the current double queue design is good. We need to prio income over
        // retries, i.e. income overtakes retries.
        if self.metrics.task_in_queue.get() > self.config.max_pending_task as i64 {
            return None;
        }

        if self.task.retry_limit == 0 {
            // The task has been retried too many times. Skip it.
            return None;
        }

        let task_hash = self.task.upstream_url();

        {
            // Deduplicate tasks.
            let mut processing_task = self.processing_task.lock().await;
            if processing_task.contains(&task_hash) {
                info!("already processing, continue to next task");
                return None;
            }
            processing_task.insert(task_hash.clone());
        }

        match self.extra {
            DownloadStageExtra::Pre { sem } => {
                // Wait for concurrency permit.
                let permit = Arc::clone(sem).acquire_owned().await.unwrap();

                let client = Cow::Owned(self.client.into_owned());
                let processing_task = Cow::Owned(self.processing_task.into_owned());
                let metrics = Cow::Owned(self.metrics.into_owned());
                let fail_tx = Cow::Owned(self.fail_tx.into_owned());
                let config = Cow::Owned(self.config.into_owned());

                Some(DownloadCtx {
                    task: self.task,
                    config,
                    metrics,
                    client,
                    processing_task,
                    fail_tx,
                    extra: DownloadStageExtra::On {
                        _permit: permit,
                        task_hash,
                    },
                })
            }
            DownloadStageExtra::On { .. } => unreachable!(),
        }
    }
}

impl DownloadCtx<'static> {
    /// Spawn the download task.
    pub fn spawn(self) {
        tokio::spawn(self.download());
    }
    /// Actual download future.
    #[instrument(skip(self), fields(storage = self.task.storage, origin = self.task.origin, path = self.task.path))]
    async fn download(self) {
        info!("start download");
        self.metrics.download_counter.inc();

        self.metrics.task_download.inc();

        let mut task_new = self.task.clone();

        info!("begin stream");
        let config = self.config.into_owned();
        let task_fut = cache_task(self.task, self.client.into_owned(), &config);
        let task_fut = tokio::time::timeout(
            std::time::Duration::from_secs(config.download_timeout),
            task_fut,
        );
        if let Err(err) = task_fut.await.unwrap_or(Err(Error::Timeout(()))) {
            warn!(error=?err, ttl=task_new.retry_limit, "failed to download task");
            task_new.retry_limit -= 1;
            self.metrics.failed_download_counter.inc();

            if !matches!(err, Error::HTTPError(_)) && !matches!(err, Error::TooLarge(_)) {
                self.fail_tx.send(task_new).unwrap();
                self.metrics.task_in_queue.inc();
            }
        };

        {
            let mut processing_task = self.processing_task.lock().await;
            match self.extra {
                DownloadStageExtra::On { task_hash, .. } => {
                    processing_task.remove(&task_hash);
                }
                DownloadStageExtra::Pre { .. } => unreachable!(),
            }
        }

        self.metrics.task_download.dec();
    }
}

/// Cache a task.
///
/// This function does the actual caching part.
/// It's called in `download_artifact`, which does something like concurrency control and retries.
async fn cache_task(task: Task, client: Client, config: &Config) -> Result<()> {
    if client
        .head(task.cached_url(config))
        .send()
        .await?
        .status()
        .is_success()
    {
        info!("already exists");
        return Ok(());
    }

    let payload = download_payload(&client, task.upstream_url(), config).await?;
    let content_length = payload.content_length();
    info!(content_length, "get length");

    let key = task.s3_key()?;

    let result = match payload {
        UploadPayload::Memory(body) => {
            stream_to_s3(
                &key,
                content_length,
                aws_sdk_s3::primitives::ByteStream::from(body),
                &config.s3,
            )
            .await?
        }
        UploadPayload::File {
            path,
            content_length,
        } => {
            let stream = match aws_sdk_s3::primitives::ByteStream::from_path(&path).await {
                Ok(stream) => stream,
                Err(err) => {
                    remove_buffer_file(&path).await;
                    return Err(Error::CustomError(format!(
                        "failed to open buffered artifact {}: {:?}",
                        path.display(),
                        err
                    )));
                }
            };

            let upload = stream_to_s3(&key, content_length, stream, &config.s3).await;
            remove_buffer_file(&path).await;
            upload?
        }
    };

    info!(?result, "upload to bucket");
    Ok(())
}

/// Download payload from URL, choosing either in-memory or file-backed buffering.
async fn download_payload(client: &Client, url: Url, config: &Config) -> Result<UploadPayload> {
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }

    let max_size = max_download_size(config);
    let file_threshold = file_threshold(config);

    match response.content_length() {
        Some(content_length) if content_length > max_size => Err(Error::TooLarge(())),
        Some(content_length) if content_length > file_threshold => {
            info!("stream mode: file backend");
            download_to_file(response, config, max_size).await
        }
        Some(_) => {
            info!("stream mode: memory buffer");
            download_to_memory(response, max_size).await
        }
        None => {
            info!("stream mode: file backend (unknown size)");
            download_to_file(response, config, max_size).await
        }
    }
}

/// Main artifact download task.
///
/// This function handles concurrency control, queueing, and retries.
pub async fn download_artifacts(
    mut rx: Receiver<Task>,
    client: Client,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) {
    let sem = Arc::new(Semaphore::new(config.concurrent_download));
    let processing_task = Arc::new(Mutex::new(HashSet::new()));
    let (fail_tx, mut fail_rx) = unbounded_channel();

    while let Some(task) = tokio::select! {
        val = fail_rx.recv() => val,
        val = rx.recv() => val
    } {
        let mut task: Task = task; // Work around intelliRust bug.

        // Apply override rules on the task.
        task.apply_override(&config.endpoints.overrides);

        let ctx = DownloadCtx {
            task,
            config: Cow::Borrowed(&config),
            metrics: Cow::Borrowed(&metrics),
            client: Cow::Borrowed(&client),
            processing_task: Cow::Borrowed(&processing_task),
            fail_tx: Cow::Borrowed(&fail_tx),
            extra: DownloadStageExtra::Pre { sem: &sem },
        };

        if let Some(ctx) = ctx.prepare().await {
            ctx.spawn();
        }
    }

    info!("artifact download stop");
}

#[cfg(test)]
mod tests {
    use httpmock::MockServer;
    use tempfile::TempDir;
    use tokio::fs;

    use super::*;

    #[tokio::test]
    async fn must_download_payload_to_memory() {
        let tmp_dir = TempDir::with_prefix("intel").unwrap();
        let config = Config {
            buffer_path: tmp_dir.path().to_path_buf(),
            file_threshold_mb: 1,
            ignore_threshold_mb: 1,
            ..Default::default()
        };

        let server = MockServer::start_async().await;
        let _mock = server.mock(|when, then| {
            when.path("/test.bin");
            then.status(200).body(b"23333333366666666");
        });

        let payload = download_payload(
            &Client::new(),
            server.url("/test.bin").parse().unwrap(),
            &config,
        )
        .await
        .unwrap();

        match payload {
            UploadPayload::Memory(data) => {
                assert_eq!(data, Bytes::from_static(b"23333333366666666"));
            }
            UploadPayload::File { .. } => panic!("must be memory-backed"),
        }
    }

    #[tokio::test]
    async fn must_download_payload_to_file() {
        let tmp_dir = TempDir::with_prefix("intel").unwrap();
        let config = Config {
            buffer_path: tmp_dir.path().to_path_buf(),
            file_threshold_mb: 0,
            ignore_threshold_mb: 1,
            ..Default::default()
        };

        let server = MockServer::start_async().await;
        let _mock = server.mock(|when, then| {
            when.path("/test.bin");
            then.status(200).body(b"23333333366666666");
        });

        let payload = download_payload(
            &Client::new(),
            server.url("/test.bin").parse().unwrap(),
            &config,
        )
        .await
        .unwrap();

        match payload {
            UploadPayload::Memory(_) => panic!("must be file-backed"),
            UploadPayload::File {
                path,
                content_length,
            } => {
                assert_eq!(content_length, 17);
                let data = fs::read(&path).await.unwrap();
                assert_eq!(data, b"23333333366666666");
                remove_buffer_file(&path).await;
            }
        }
    }

    #[tokio::test]
    async fn must_reject_large_payload() {
        let tmp_dir = TempDir::with_prefix("intel").unwrap();
        let config = Config {
            buffer_path: tmp_dir.path().to_path_buf(),
            file_threshold_mb: 0,
            ignore_threshold_mb: 0,
            ..Default::default()
        };

        let server = MockServer::start_async().await;
        let _mock = server.mock(|when, then| {
            when.path("/test.bin");
            then.status(200).body(b"1");
        });

        let result = download_payload(
            &Client::new(),
            server.url("/test.bin").parse().unwrap(),
            &config,
        )
        .await;
        assert!(matches!(result, Err(Error::TooLarge(()))));
    }
}
