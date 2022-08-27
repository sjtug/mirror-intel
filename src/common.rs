//! Common types.

use std::sync::Arc;

use prometheus::{proto, IntCounter as Counter, IntGauge as Gauge, Opts, Registry};
use reqwest::Client;
use rocket::response;
use rusoto_s3::S3Client;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use url::Url;

use crate::{Error, Result};

/// A cache task.
#[derive(Debug, Clone)]
pub struct Task {
    /// S3 storage prefix.
    pub storage: &'static str,
    /// Upstream origin.
    pub origin: String,
    /// File path.
    pub path: String,
    /// Max allowed retries.
    pub retry_limit: usize,
}

impl Task {
    /// Upstream url.
    pub fn upstream_url(&self) -> Url {
        Url::parse(&format!("{}/{}", self.origin, self.path)).expect("invalid upstream url")
    }

    /// S3 cache url.
    pub fn cached_url(&self, config: &Config) -> Url {
        Url::parse(&format!(
            "{}/{}/{}/{}",
            config.s3.endpoint, config.s3.bucket, self.storage, self.path
        ))
        .expect("invalid cached url")
    }

    /// S3 root path.
    pub fn root_path(&self) -> String {
        format!("/{}/{}", self.storage, self.path)
    }

    /// S3 key. Percent-encoded path is decoded.
    pub fn s3_key(&self) -> Result<String> {
        Ok(format!(
            "{}/{}",
            self.storage,
            rocket::http::uri::Uri::percent_decode(self.path.as_bytes())
                .map_err(|_| Error::DecodePathError(()))?
        ))
    }

    /// Apply upstream URL override rules on the task.
    pub fn apply_override(&mut self, overrides: &[EndpointOverride]) {
        for endpoint_override in overrides {
            if self.origin.contains(&endpoint_override.pattern) {
                self.origin = self
                    .origin
                    .replace(&endpoint_override.pattern, &endpoint_override.replace);
            }
        }
    }
}

/// Prometheus metrics.
pub struct Metrics {
    /// Count of resolved objects.
    pub resolve_counter: Counter,
    /// Count of objects downloaded from origin.
    pub download_counter: Counter,
    /// Count of objects failed to download.
    pub failed_download_counter: Counter,
    /// Tasks in queue.
    pub task_in_queue: Gauge,
    /// Tasks in progress.
    pub task_download: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        let resolve_counter =
            Counter::with_opts(Opts::new("resolve_counter", "resolved objects")).unwrap();
        let download_counter = Counter::with_opts(Opts::new(
            "download_counter",
            "objects downloaded from origin",
        ))
        .unwrap();
        let failed_download_counter = Counter::with_opts(Opts::new(
            "failed_download_counter",
            "objects failed to download",
        ))
        .unwrap();
        let task_in_queue = Gauge::with_opts(Opts::new("task_in_queue", "tasks in queue")).unwrap();
        let task_download =
            Gauge::with_opts(Opts::new("task_download", "tasks processing")).unwrap();

        Self {
            resolve_counter,
            download_counter,
            failed_download_counter,
            task_in_queue,
            task_download,
        }
    }
}

impl Metrics {
    pub fn gather(&self) -> Vec<proto::MetricFamily> {
        let registry = Registry::new();
        registry
            .register(Box::new(self.resolve_counter.clone()))
            .unwrap();
        registry
            .register(Box::new(self.download_counter.clone()))
            .unwrap();
        registry
            .register(Box::new(self.failed_download_counter.clone()))
            .unwrap();

        registry
            .register(Box::new(self.task_in_queue.clone()))
            .unwrap();
        registry
            .register(Box::new(self.task_download.clone()))
            .unwrap();
        registry.gather()
    }
}

/// Runtime hub.
#[derive(Clone)]
pub struct IntelMission {
    /// Sender to caching future.
    ///
    /// You may issue a new caching task by sending it to the caching future.
    ///
    /// This field can be `None` if mirror-intel is in read-only mode.
    pub tx: Option<Sender<Task>>,
    /// Reqwest client.
    pub client: Client,
    /// Prometheus metrics.
    pub metrics: Arc<Metrics>,
    /// S3 client.
    ///
    /// This is an anonymous client.
    pub s3_client: Arc<S3Client>,
}

/// An upstream endpoint override rule.
#[derive(Clone, Deserialize, Debug)]
pub struct EndpointOverride {
    /// Name of the rule.
    ///
    /// Currently this field is only used for a descriptive purpose.
    pub name: String,
    /// Pattern to match against the origin.
    ///
    /// Note that only plain strings are supported, i.e. no regex, and substring matching is allowed.
    pub pattern: String,
    /// Replacement for the matched pattern.
    pub replace: String,
}

/// Endpoints of origin servers.
#[derive(Default, Clone, Deserialize, Debug)]
pub struct Endpoints {
    pub rust_static: String,
    pub homebrew_bottles: String,
    pub pypi_packages: String,
    pub fedora_iot: String,
    pub fedora_ostree: String,
    pub flathub: String,
    pub crates_io: String,
    pub dart_pub: String,
    pub guix: String,
    pub pytorch_wheels: String,
    pub linuxbrew_bottles: String,
    pub sjtug_internal: String,
    pub flutter_infra: String,
    pub flutter_infra_release: String,
    pub github_release: String,
    pub nix_channels_store: String,
    pub pypi_simple: String,
    pub opam_cache: String,
    pub gradle_distribution: String,
    /// Upstream override rules.
    pub overrides: Vec<EndpointOverride>,
    /// Paths starts with any of these prefixes will be unconditionally redirected to S3 storage.
    pub s3_only: Vec<String>,
}

/// Configuration for S3 storage.
#[derive(Default, Clone, Deserialize, Debug)]
pub struct S3Config {
    /// Endpoint of the S3 service.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
}

/// Configuration for Github Release endpoint.
#[derive(Default, Clone, Deserialize, Debug)]
pub struct GithubReleaseConfig {
    /// Repositories allowed to be cached.
    ///
    /// Accessing a repository that is not in this list will result in an unconditional redirect.
    pub allow: Vec<String>,
}

/// Global application config.
#[derive(Default, Clone, Deserialize, Debug)]
pub struct Config {
    /// Max pending task allowed.
    ///
    /// If the count of pending tasks exceeds this value, early tasks will be ignored.
    pub max_pending_task: usize,
    /// Max concurrent download tasks allowed.
    pub concurrent_download: usize,
    /// Upstream endpoints for redirecting, reverse-proxying and caching.
    pub endpoints: Endpoints,
    /// S3 storage to store cached files.
    pub s3: S3Config,
    /// User agent for requests to upstream.
    pub user_agent: String,
    /// Max size of a stream (usually upstream file) to be buffered in memory when processing a task.
    ///
    /// If the stream is larger than this value, it will be backed by a temporary file.
    pub file_threshold_mb: u64,
    /// Any stream (usually upstream file) larger than this value will be ignored
    /// and won't be downloaded when processing a task.
    pub ignore_threshold_mb: u64,
    /// Base URL of this server.
    pub base_url: String,
    /// Max retry times for a failed download.
    /// It's named as `ttl` for backward compatibility.
    #[serde(rename = "ttl")]
    pub max_retries: usize,
    /// Maximum size of a cached file to be served directly instead of redirect
    ///
    /// Only works in `dart_pub` currently.
    pub direct_stream_size_kb: u64,
    /// Run in read-only mode.
    ///
    /// Do not write to S3 storage.
    pub read_only: bool,
    /// Timeout for upstream requests.
    pub download_timeout: u64,
    /// Github release related configs.
    pub github_release: GithubReleaseConfig,
}

#[derive(Debug, Responder)]
pub enum IntelResponse<'a> {
    Redirect(response::Redirect),
    Response(response::Response<'a>),
}

macro_rules! impl_from {
    ($tt: ty, $struct: ty, $variant: expr) => {
        impl<'a> From<$tt> for $struct {
            fn from(res: $tt) -> Self {
                $variant(res)
            }
        }
    };
}

impl_from! { response::Redirect, IntelResponse<'a>, IntelResponse::Redirect }
impl_from! { response::Response<'a>, IntelResponse<'a>, IntelResponse::Response }

/// Intel object to be served to client.
///
/// Resolve result of a task.
pub enum IntelObject {
    /// Cache hit.
    Cached { task: Task, resp: reqwest::Response },
    /// Cache miss.
    Origin { task: Task },
}
