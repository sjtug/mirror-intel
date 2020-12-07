use crate::common::{IntelMission, Task, S3_BUCKET};
use crate::error::{Error, Result};

use std::path::PathBuf;

use reqwest::StatusCode;
use rocket::response::Redirect;

pub fn resolve_ostree(origin: &str, path: &str) -> Option<Redirect> {
    if path.starts_with("summary") || path.starts_with("config") || path.starts_with("refs/") {
        return Some(Redirect::moved(format!("{}/{}", origin, path)));
    }
    None
}

pub fn decode_path(path: &PathBuf) -> Result<&str> {
    Ok(path.to_str().ok_or_else(|| Error::DecodePathError(()))?)
}

pub async fn resolve_object(
    storage: &str,
    path: &str,
    origin: &str,
    mission: &IntelMission,
) -> Result<Redirect> {
    mission.metrics.resolve_counter.inc();

    let s3 = format!(
        "https://s3.jcloud.sjtu.edu.cn/{}/{}/{}",
        S3_BUCKET, storage, path
    );
    let origin = format!("{}/{}", origin, path);
    if let Ok(resp) = mission.client.head(&s3).send().await {
        match resp.status() {
            StatusCode::OK => return Ok(Redirect::found(s3)),
            StatusCode::FORBIDDEN => {
                mission
                    .tx
                    .clone()
                    .send(Task {
                        storage: storage.to_string(),
                        path: path.to_string(),
                        origin: origin.to_string(),
                        ttl: 3,
                    })
                    .await
                    .map_err(|_| Error::SendError(()))?;
                mission.metrics.task_in_queue.inc();
            }
            _ => {}
        }
    }
    Ok(Redirect::found(origin))
}
