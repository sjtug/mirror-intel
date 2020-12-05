use reqwest::Client;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct Task {
    pub storage: String,
    pub origin: String,
    pub path: String,
    pub ttl: usize
}

#[derive(Clone)]
pub struct IntelMission {
    pub tx: Sender<Task>,
    pub client: Client,
}

pub const MAX_PENDING_TASK: usize = 1024 * 16;
pub const MAX_CONCURRENT_DOWNLOAD: usize = 256;

pub const S3_BUCKET: &str = "899a892efef34b1b944a19981040f55b-oss01";
