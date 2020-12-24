use prometheus::{IntCounter as Counter, IntGauge as Gauge, Opts, Registry};
use reqwest::Client;

use rocket::response;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Task {
    pub storage: &'static str,
    pub origin: String,
    pub path: String,
    pub ttl: usize,
}

impl Task {
    pub fn upstream(&self) -> String {
        format!("{}/{}", self.origin, self.path)
    }

    pub fn cached(&self, config: &Config) -> String {
        format!(
            "{}/{}/{}/{}",
            config.s3.endpoint, config.s3.bucket, self.storage, self.path
        )
    }

    pub fn root_path(&self) -> String {
        format!("/{}/{}", self.storage, self.path)
    }

    pub fn to_download_task(&mut self, overrides: &[EndpointOverride]) {
        for endpoint_override in overrides {
            if self.origin.contains(&endpoint_override.pattern) {
                self.origin = self
                    .origin
                    .replace(&endpoint_override.pattern, &endpoint_override.replace);
            }
        }
    }
}

pub struct Metrics {
    pub resolve_counter: Counter,
    pub download_counter: Counter,
    pub failed_download_counter: Counter,
    pub task_in_queue: Gauge,
    pub task_download: Gauge,
    pub registry: Registry,
}

impl Metrics {
    pub fn new() -> Self {
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

        let registry = Registry::new();
        registry
            .register(Box::new(resolve_counter.clone()))
            .unwrap();
        registry
            .register(Box::new(download_counter.clone()))
            .unwrap();
        registry
            .register(Box::new(failed_download_counter.clone()))
            .unwrap();

        registry.register(Box::new(task_in_queue.clone())).unwrap();
        registry.register(Box::new(task_download.clone())).unwrap();

        Self {
            registry,
            resolve_counter,
            download_counter,
            task_in_queue,
            task_download,
            failed_download_counter,
        }
    }
}

#[derive(Clone)]
pub struct IntelMission {
    pub tx: Sender<Task>,
    pub client: Client,
    pub metrics: Arc<Metrics>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct EndpointOverride {
    pub name: String,
    pub pattern: String,
    pub replace: String,
}

#[derive(Clone, Deserialize, Debug)]
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
    pub overrides: Vec<EndpointOverride>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Config {
    pub max_pending_task: usize,
    pub concurrent_download: usize,
    pub endpoints: Endpoints,
    pub s3: S3Config,
    pub user_agent: String,
    pub file_threshold_mb: u64,
    pub ignore_threshold_mb: u64,
    pub base_url: String,
    pub ttl: usize,
    pub direct_stream_size_kb: u64,
    pub read_only: bool,
    pub download_timeout: u64,
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

pub enum IntelObject {
    Cached { task: Task, resp: reqwest::Response },
    Origin { task: Task },
}
