//! Artifact download implementation.

use std::borrow::Cow;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use pin_project::pin_project;
use reqwest::{Client, Response};
use tap::Pipe;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc::{unbounded_channel, Receiver, UnboundedSender};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::codec;
use tracing::{debug, info, instrument, warn};
use url::Url;

use crate::common::{Config, Metrics, Task};
use crate::error::{Error, Result};
use crate::storage::stream_to_s3;

type IOResult = std::result::Result<Bytes, std::io::Error>;

/// Convert reqwest resp stream to io result stream.
fn into_io_stream(
    stream: impl Stream<Item = reqwest::Result<Bytes>>,
) -> impl Stream<Item = IOResult> {
    stream.map(move |x| {
        x.map_err(|err| {
            warn!("failed to receive data: {:?}", err);
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
    async fn into_bytes_stream(mut self) -> Result<impl Stream<Item = IOResult>> {
        // remove file on disk, but we could still read it
        let mut f = self.f.take().unwrap();
        f.flush().await?;
        let mut f = f.into_inner();
        if let Err(err) = fs::remove_file(&self.path).await {
            warn!("failed to remove cache file: {:?} {:?}", err, self.path);
        }
        f.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(
            codec::FramedRead::new(BufReader::new(f), codec::BytesCodec::new())
                .map_ok(BytesMut::freeze),
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
async fn into_file_stream(
    mut stream: impl Stream<Item = IOResult> + Unpin,
    config: &Config,
) -> Result<impl Stream<Item = IOResult>> {
    let path = config.buffer_path.join(
        FILE_ID
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .to_string(),
    );
    let mut file = FileWrapper::open(&path).await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        file.as_mut().write_all(&v).await?;
    }
    file.into_bytes_stream().await
}

/// Convert a stream of bytes to a memory-backed one.
///
/// The old stream is consumed and a new stream with the same contents is returned.
/// It can be used to download small files from the network.
async fn into_memory_stream(
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
            warn!("{:?}, ttl={}", err, task_new.retry_limit);
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
    let (content_length, stream) = stream_from_url(client, task.upstream_url(), config).await?;
    info!("get length={}", content_length);
    let key = task.s3_key()?;
    let result = stream_to_s3(
        &key,
        content_length,
        rusoto_s3::StreamingBody::new(stream),
        &config.s3,
    )
    .await?;
    info!("upload to bucket");
    debug!("{:?}", result);
    Ok(())
}

#[pin_project(project = EitherProj)]
enum Either<T, U> {
    Left(#[pin] T),
    Right(#[pin] U),
}

impl<O, T, U> Stream for Either<T, U>
where
    T: Stream<Item = O>,
    U: Stream<Item = O>,
{
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EitherProj::Left(left) => left.poll_next(cx),
            EitherProj::Right(right) => right.poll_next(cx),
        }
    }
}

/// Convert this response into a byte stream.
///
/// Backing buffer type is chosen based on the content length.
async fn into_stream(
    resp: Response,
    config: &Config,
) -> Result<(u64, impl Stream<Item = IOResult> + Send + Sync)> {
    if let Some(content_length) = resp.content_length() {
        if content_length > config.ignore_threshold_mb * 1024 * 1024 {
            Err(Error::TooLarge(()))
        } else {
            let io_stream = resp.bytes_stream().pipe(into_io_stream);
            if content_length > config.file_threshold_mb * 1024 * 1024 {
                info!("stream mode: file backend");
                let stream = io_stream.pipe(|s| into_file_stream(s, config)).await?;
                Ok((content_length, Either::Left(Either::Left(stream))))
            } else if content_length > 1024 * 1024 {
                info!("stream mode: memory cache");
                let stream = io_stream
                    .pipe(|s| into_memory_stream(content_length as usize, s))
                    .await?;
                Ok((content_length, Either::Left(Either::Right(stream))))
            } else {
                info!("stream mode: direct copy");
                Ok((content_length, Either::Right(Either::Left(io_stream))))
            }
        }
    } else {
        info!("stream mode: direct copy");
        let resp = resp.bytes().await?;
        Ok((
            resp.len() as u64,
            Either::Right(Either::Right(futures::stream::iter(vec![resp]).map(Ok))),
        ))
    }
}

/// Download a stream of bytes from the given url.
async fn stream_from_url(
    client: Client,
    url: Url,
    config: &Config,
) -> Result<(u64, impl Stream<Item = IOResult> + Send + Sync)> {
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    into_stream(response, config).await
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
    use futures_util::stream;
    use futures_util::stream::StreamExt;
    use httpmock::MockServer;
    use tempdir::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_overlay_file_create() {
        let tmp_dir = TempDir::new("intel").unwrap();
        let path = tmp_dir.path().join("test.bin");
        let mut wrapper = FileWrapper::open(&path).await.unwrap();
        wrapper.as_mut().write_all(b"233333333").await.unwrap();
        let mut stream = wrapper.into_bytes_stream().await.unwrap();
        assert_eq!(&stream.next().await.unwrap().unwrap(), "233333333");
    }

    #[tokio::test]
    async fn must_into_file_stream() {
        let tmp_dir = TempDir::new("intel").unwrap();
        let config = Config {
            buffer_path: tmp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let data = [
            Ok(Bytes::from_static(b"233333333")),
            Ok(Bytes::from_static(b"666666666")),
        ];

        let stream = stream::iter(data);
        let file_stream = into_file_stream(stream, &config).await.unwrap();

        // Chunking is not guaranteed to be preserved.
        let expected = b"233333333666666666";
        let output = file_stream
            .fold(Vec::new(), |mut acc, b| async move {
                acc.extend_from_slice(&b.unwrap());
                acc
            })
            .await;
        assert_eq!(output, expected);
    }

    #[tokio::test]
    async fn must_into_memory_stream() {
        let data = [
            Ok(Bytes::from_static(b"233333333")),
            Ok(Bytes::from_static(b"666666666")),
        ];

        let stream = stream::iter(data);
        let file_stream = into_memory_stream(100, stream).await.unwrap();

        // A single chunk is returned.
        let expected = vec![Bytes::from_static(b"233333333666666666")];
        let output: Vec<_> = file_stream.map(|b| b.unwrap()).collect().await;
        assert_eq!(output, expected);
    }

    #[tokio::test]
    async fn must_from_url() {
        let tmp_dir = TempDir::new("intel").unwrap();
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

        let (length, stream) = stream_from_url(
            Client::new(),
            server.url("/test.bin").parse().unwrap(),
            &config,
        )
        .await
        .unwrap();
        assert_eq!(length, 17);
        let data = stream
            .fold(Vec::new(), |mut acc, b| async move {
                acc.extend_from_slice(&b.unwrap());
                acc
            })
            .await;
        assert_eq!(data, b"23333333366666666");
    }
}
