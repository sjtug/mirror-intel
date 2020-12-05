use reqwest::Client;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct Task {
    pub storage: String,
    pub origin: String,
    pub path: String,
    pub ttl: usize,
}

#[derive(Clone)]
pub struct IntelMission {
    pub tx: Sender<Task>,
    pub client: Client,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub max_pending_task: usize,
    pub concurrent_download: usize,
}

pub const S3_BUCKET: &str = "899a892efef34b1b944a19981040f55b-oss01";
