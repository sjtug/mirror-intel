use prometheus::{IntCounter as Counter, IntGauge as Gauge, Opts, Registry};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct Task {
    pub storage: String,
    pub origin: String,
    pub path: String,
    pub ttl: usize,
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
pub struct Endpoints {
    pub rustup: String,
    pub homebrew_bottles: String,
    pub pypi_packages: String,
    pub fedora_iot: String,
    pub fedora_ostree: String,
    pub flathub: String,
    pub crates_io: String,
    pub dart_pub: String,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Config {
    pub max_pending_task: usize,
    pub concurrent_download: usize,
    pub endpoints: Endpoints,
    pub user_agent: String,
    pub file_threshold_mb: u64,
    pub ignore_threshold_mb: u64,
    pub base_url: String,
}

pub const S3_BUCKET: &str = "899a892efef34b1b944a19981040f55b-oss01";
