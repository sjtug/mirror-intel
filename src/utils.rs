use crate::common::{IntelMission, IntelObject, IntelResponse, Task};
use crate::error::{Error, Result};

use std::io::Cursor;
use std::path::PathBuf;

use futures::stream::TryStreamExt;
use response::ResponseBuilder;
use rocket::{
    http::ContentType,
    response::{self, Content, Redirect},
    Response,
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

pub fn ostree_ignore(path: &str) -> bool {
    path.starts_with("summary") || path.starts_with("config") || path.starts_with("refs/")
}

pub fn decode_path(path: &PathBuf) -> Result<&str> {
    Ok(path.to_str().ok_or_else(|| Error::DecodePathError(()))?)
}

impl Task {
    pub async fn resolve(self, mission: &IntelMission) -> Result<IntelObject> {
        mission.metrics.resolve_counter.inc();
        if let Ok(resp) = mission.client.get(&self.cached()).send().await {
            if resp.status().is_success() {
                return Ok(IntelObject::Cached { task: self, resp });
            } else {
                mission.metrics.task_in_queue.inc();
                mission
                    .tx
                    .clone()
                    .send(self.clone())
                    .await
                    .map_err(|_| Error::SendError(()))?;
            }
        }

        Ok(IntelObject::Origin { task: self })
    }

    pub fn resolve_upstream(self) -> IntelObject {
        IntelObject::Origin { task: self }
    }
}

impl IntelObject {
    pub fn task(&self) -> &Task {
        match self {
            IntelObject::Cached { task, .. } => task,
            IntelObject::Origin { task, .. } => task,
        }
    }

    pub fn target(&self) -> String {
        match self {
            IntelObject::Cached { task, .. } => task.cached(),
            IntelObject::Origin { task } => task.upstream(),
        }
    }

    pub fn redirect(self) -> Redirect {
        match &self {
            IntelObject::Cached { .. } => Redirect::moved(self.target()),
            IntelObject::Origin { .. } => Redirect::found(self.target()),
        }
    }

    pub async fn rewrite_upstream(
        self,
        intel_mission: &IntelMission,
        below_size_kb: u64,
        f: impl Fn(String) -> String,
    ) -> Result<IntelResponse<'static>> {
        let task = self.task();
        let upstream = task.upstream();
        let resp = intel_mission.client.get(&upstream).send().await?;

        if resp.status().is_success() {
            if let Some(content_length) = resp.content_length() {
                if content_length <= below_size_kb * 1024 {
                    let text = resp.text().await?;
                    let text = f(text);
                    return Ok(Response::build()
                        .streamed_body(Cursor::new(text))
                        .finalize()
                        .into());
                }
            }
        }

        Ok(self.redirect().into())
    }

    fn set_status(intel_response: &mut ResponseBuilder, response: &reqwest::Response) {
        let status = response.status();
        if let Some(reason) = status.canonical_reason() {
            intel_response.status(rocket::http::Status::new(status.as_u16(), reason));
        } else {
            intel_response.status(rocket::http::Status::new(status.as_u16(), ""));
        }
    }

    pub async fn reverse_proxy(
        self,
        intel_mission: &IntelMission,
    ) -> Result<IntelResponse<'static>> {
        match self {
            IntelObject::Cached { resp, .. } => {
                let mut intel_response = Response::build();
                if let Some(content_length) = resp.content_length() {
                    intel_response.raw_header("content-length", content_length.to_string());
                }
                Self::set_status(&mut intel_response, &resp);
                intel_response.streamed_body(
                    resp.bytes_stream()
                        .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                        .into_async_read()
                        .compat(),
                );
                Ok(intel_response.finalize().into())
            }
            IntelObject::Origin { ref task } => {
                let resp = intel_mission.client.get(&task.upstream()).send().await?;
                let mut intel_response = Response::build();
                if let Some(content_length) = resp.content_length() {
                    intel_response.raw_header("content-length", content_length.to_string());
                }
                Self::set_status(&mut intel_response, &resp);
                intel_response.streamed_body(
                    resp.bytes_stream()
                        .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                        .into_async_read()
                        .compat(),
                );
                Ok(intel_response.finalize().into())
            }
        }
    }
    pub async fn stream_small_cached(
        self,
        size_kb: u64,
        intel_mission: &IntelMission,
    ) -> Result<IntelResponse<'static>> {
        match &self {
            IntelObject::Cached { resp, .. } => {
                if let Some(content_length) = resp.content_length() {
                    if content_length <= size_kb * 1024 {
                        debug!("{} <= {}, direct stream", content_length, size_kb * 1024);
                        return Ok(self.reverse_proxy(intel_mission).await?.into());
                    }
                }
                Ok(self.redirect().into())
            }
            IntelObject::Origin { .. } => Ok(self.redirect().into()),
        }
    }
}

impl<'a> IntelResponse<'a> {
    pub fn content_type(mut self, content_type: ContentType) -> Self {
        match &mut self {
            IntelResponse::Redirect(_) => {}
            IntelResponse::Response(resp) => {
                resp.set_header(content_type);
            }
        }
        self
    }
}
