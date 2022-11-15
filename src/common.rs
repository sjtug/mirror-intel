//! Common types.

use std::path::PathBuf;
use std::sync::Arc;

use actix_web::body::EitherBody;
use actix_web::http::{header, StatusCode};
use actix_web::{HttpRequest, HttpResponse, Responder};
use figment::providers::{Format, Serialized, Toml};
use figment::Figment;
use percent_encoding::percent_decode;
use prometheus::{proto, IntCounter as Counter, IntGauge as Gauge, Opts, Registry};
use reqwest::Client;
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
            config.s3.website_endpoint, config.s3.bucket, self.storage, self.path
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
            percent_decode(self.path.as_bytes())
                .decode_utf8()
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
#[derive(Clone, Deserialize, Debug, Eq, PartialEq)]
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
#[derive(Default, Clone, Deserialize, Debug, Eq, PartialEq)]
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
    pub julia: String,
    /// Upstream override rules.
    pub overrides: Vec<EndpointOverride>,
    /// Paths starts with any of these prefixes will be unconditionally redirected to S3 storage.
    pub s3_only: Vec<String>,
}

/// Configuration for S3 storage.
#[derive(Default, Clone, Deserialize, Debug, Eq, PartialEq)]
pub struct S3Config {
    /// Name of the S3 storage.
    pub name: String,
    /// S3 endpoint of the storage service.
    pub endpoint: String,
    /// Website endpoint of the S3 service.
    pub website_endpoint: String,
    /// Bucket name.
    pub bucket: String,
}

/// Configuration for Github Release endpoint.
#[derive(Default, Clone, Deserialize, Debug, Eq, PartialEq)]
pub struct GithubReleaseConfig {
    /// Repositories allowed to be cached.
    ///
    /// Accessing a repository that is not in this list will result in an unconditional redirect.
    pub allow: Vec<String>,
}

/// Global application config.
#[derive(Default, Clone, Deserialize, Debug, Eq, PartialEq)]
pub struct Config {
    /// Address to listen on.
    pub address: String,
    /// Port to listen on.
    pub port: u16,
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
    /// Path of temporary buffer directory.
    pub buffer_path: PathBuf,
    /// Worker tasks to serve requests.
    pub workers: Option<usize>,
}

/// An empty redirect response to a given URL.
pub enum Redirect {
    /// Construct a “permanent” (308) redirect response.
    Permanent(String),
    /// Construct a “temporary” (307) redirect response.
    Temporary(String),
}

impl From<Redirect> for HttpResponse {
    fn from(this: Redirect) -> Self {
        let (code, location) = match this {
            Redirect::Permanent(url) => (StatusCode::PERMANENT_REDIRECT, url),
            Redirect::Temporary(url) => (StatusCode::TEMPORARY_REDIRECT, url),
        };
        Self::build(code)
            .insert_header((header::LOCATION, location))
            .finish()
    }
}

impl Responder for Redirect {
    type Body = ();

    fn respond_to(self, _: &HttpRequest) -> HttpResponse<Self::Body> {
        HttpResponse::from(self).drop_body()
    }
}

/// Convenient struct for response body.
pub enum IntelResponse {
    /// Redirect to a given URL.
    Redirect(Redirect),
    /// Any other response.
    Response(HttpResponse),
}

impl Responder for IntelResponse {
    type Body = EitherBody<()>;

    fn respond_to(self, req: &HttpRequest) -> HttpResponse<Self::Body> {
        match self {
            Self::Redirect(redirect) => redirect.respond_to(req).map_into_left_body(),
            Self::Response(resp) => resp.respond_to(req).map_into_right_body(),
        }
    }
}

impl From<IntelResponse> for HttpResponse {
    fn from(resp: IntelResponse) -> Self {
        match resp {
            IntelResponse::Redirect(redirect) => redirect.into(),
            IntelResponse::Response(resp) => resp,
        }
    }
}

macro_rules! impl_from {
    ($ty: ty, $struct: ty, $variant: expr) => {
        impl From<$ty> for $struct {
            fn from(res: $ty) -> Self {
                $variant(res)
            }
        }
    };
}

impl_from! { Redirect, IntelResponse, IntelResponse::Redirect }
impl_from! { HttpResponse, IntelResponse, IntelResponse::Response }

/// Intel object to be served to client.
///
/// Resolve result of a task.
pub enum IntelObject {
    /// Cache hit.
    Cached { task: Task, resp: reqwest::Response },
    /// Cache miss.
    Origin { task: Task },
}

pub fn collect_config() -> Config {
    let figment = Figment::new()
        .merge(Serialized::default("address", "127.0.0.1"))
        .merge(Serialized::default("port", 8000))
        .merge(Toml::file("Rocket.toml").nested()) // For backward compatibility
        .merge(Toml::file("mirror-intel.toml").nested());
    figment.extract().expect("config")
}

#[cfg(test)]
mod tests {
    use figment::Jail;

    use crate::common::{
        collect_config, EndpointOverride, Endpoints, GithubReleaseConfig, S3Config,
    };
    use crate::Config;

    #[test]
    fn must_collect_config() {
        const MIRROR_INTEL_TOML: &str = include_str!("../tests/config/mirror-intel.toml");
        const ROCKET_TOML: &str = include_str!("../tests/config/Rocket.toml");
        Jail::expect_with(|jail| {
            jail.create_file("mirror-intel.toml", MIRROR_INTEL_TOML)?;
            jail.create_file("Rocket.toml", ROCKET_TOML)?;
            let config = collect_config();
            let expected = Config {
                address: "0.0.0.0".into(), // Default values can be overridden.
                port: 8000,                // Default values takes effect if not overridden.
                max_pending_task: 16384,   // Values in Rocket.toml must be included.
                concurrent_download: 512,  // mirror-intel.toml takes precedence.
                endpoints: Endpoints {
                    rust_static: "https://mirrors.tuna.tsinghua.edu.cn/rustup".into(),
                    homebrew_bottles: "https://homebrew.bintray.com".into(),
                    pypi_packages: "https://mirrors.bfsu.edu.cn/pypi/web/packages".into(),
                    fedora_iot: "https://d2ju0wfl996cmc.cloudfront.net".into(),
                    fedora_ostree: "https://d2uk5hbyrobdzx.cloudfront.net".into(),
                    flathub: "https://dl.flathub.org/repo".into(),
                    crates_io: "https://static.crates.io".into(),
                    dart_pub: "https://mirrors.tuna.tsinghua.edu.cn/dart-pub".into(),
                    guix: "https://ci.guix.gnu.org".into(),
                    pytorch_wheels: "https://download.pytorch.org/whl".into(),
                    linuxbrew_bottles: "https://linuxbrew.bintray.com".into(),
                    sjtug_internal: "https://github.com/sjtug".into(),
                    flutter_infra: "https://storage.flutter-io.cn/flutter_infra".into(),
                    flutter_infra_release: "https://storage.flutter-io.cn/flutter_infra_release"
                        .into(),
                    github_release: "https://github.com".into(),
                    nix_channels_store: "https://mirrors.tuna.tsinghua.edu.cn/nix-channels/store"
                        .into(),
                    pypi_simple: "https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple".into(),
                    opam_cache: "https://opam.ocaml.org/cache".into(),
                    gradle_distribution: "https://services.gradle.org/distributions".into(),
                    julia: "https://kr.storage.juliahub.com".into(),
                    overrides: vec![
                        EndpointOverride {
                            name: "flutter".into(),
                            pattern: "https://storage.flutter-io.cn/".into(),
                            replace: "https://storage.googleapis.com/".into(),
                        },
                        EndpointOverride {
                            name: "tuna".into(),
                            pattern: "https://mirrors.tuna.tsinghua.edu.cn/".into(),
                            replace: "https://nanomirrors.tuna.tsinghua.edu.cn/".into(),
                        },
                    ],
                    s3_only: vec!["voidlinux/".into()],
                },
                s3: S3Config {
                    name: "jCloud S3".into(),
                    endpoint: "https://s3.jcloud.sjtu.edu.cn".into(),
                    website_endpoint: "https://s3.jcloud.sjtu.edu.cn".into(),
                    bucket: "899a892efef34b1b944a19981040f55b-oss01".into(),
                },
                user_agent: "mirror-intel / 0.1 (siyuan.internal.sjtug.org)".into(),
                file_threshold_mb: 4,
                ignore_threshold_mb: 1024,
                base_url: "https://mirrors.sjtug.sjtu.edu.cn".into(),
                max_retries: 3,
                direct_stream_size_kb: 4,
                read_only: false,
                download_timeout: 3600,
                github_release: GithubReleaseConfig {
                    allow: vec!["sjtug/lug/".into(), "FreeCAD/FreeCAD/".into()],
                },
                buffer_path: "/mnt/cache/".into(),
                workers: None,
            };
            assert_eq!(config, expected);
            Ok(())
        });
    }
}
