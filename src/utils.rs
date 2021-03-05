use crate::common::{Config, IntelMission, IntelObject, IntelResponse, Task};
use crate::error::{Error, Result};

use std::io::Cursor;

use futures::stream::TryStreamExt;
use response::ResponseBuilder;
use rocket::{
    http::ContentType,
    response::{self, Redirect},
    Response,
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

impl Task {
    async fn resolve_internal(
        self,
        mission: &IntelMission,
        config: &Config,
        head: bool,
    ) -> Result<IntelObject> {
        mission.metrics.resolve_counter.inc();
        let req = if head {
            mission.client.head(&self.cached(config))
        } else {
            mission.client.get(&self.cached(config))
        };
        if let Ok(resp) = req.send().await {
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

    pub async fn resolve(self, mission: &IntelMission, config: &Config) -> Result<IntelObject> {
        self.resolve_internal(mission, config, false).await
    }

    pub async fn resolve_no_content(
        self,
        mission: &IntelMission,
        config: &Config,
    ) -> Result<IntelObject> {
        self.resolve_internal(mission, config, true).await
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

    pub fn target(&self, config: &crate::common::Config) -> String {
        match self {
            IntelObject::Cached { task, .. } => task.cached(config),
            IntelObject::Origin { task } => task.upstream(),
        }
    }

    pub fn redirect(self, config: &crate::common::Config) -> Redirect {
        match &self {
            IntelObject::Cached { .. } => Redirect::moved(self.target(config)),
            IntelObject::Origin { .. } => Redirect::found(self.target(config)),
        }
    }

    pub async fn rewrite_upstream(
        self,
        intel_mission: &IntelMission,
        below_size_kb: u64,
        f: impl Fn(String) -> String,
        config: &Config,
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

        Ok(self.redirect(config).into())
    }

    fn set_status(intel_response: &mut ResponseBuilder, response: &reqwest::Response) {
        let status = response.status();
        // special case for NGINX 499
        if status.as_u16() == 499 {
            intel_response.status(rocket::http::Status::NotFound);
        } else if let Some(reason) = status.canonical_reason() {
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
                if let Some(content_type) = resp.headers().get(reqwest::header::CONTENT_TYPE) {
                    if let Ok(content_type) = content_type.to_str() {
                        intel_response.raw_header("content-type", content_type.to_string());
                    }
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
                if let Some(content_type) = resp.headers().get(reqwest::header::CONTENT_TYPE) {
                    if let Ok(content_type) = content_type.to_str() {
                        intel_response.raw_header("content-type", content_type.to_string());
                    }
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
        config: &Config,
    ) -> Result<IntelResponse<'static>> {
        match &self {
            IntelObject::Cached { resp, .. } => {
                if let Some(content_length) = resp.content_length() {
                    if content_length <= size_kb * 1024 {
                        debug!("{} <= {}, direct stream", content_length, size_kb * 1024);
                        return Ok(self.reverse_proxy(intel_mission).await?.into());
                    }
                }
                Ok(self.redirect(config).into())
            }
            IntelObject::Origin { .. } => Ok(self.redirect(config).into()),
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

#[catch(404)]
pub fn not_found(req: &rocket::Request) -> Response<'static> {
    no_route_for(&req.uri().to_string())
}

pub fn no_route_for(mut route: &str) -> Response<'static> {
    let mut resp = Response::build();
    if route.ends_with("/") {
        route = &route[..route.len() - 1];
    }
    if route.starts_with("/") {
        route = &route[1..];
    }
    let body = format!(
        r#"<p>No route for {}.</p>
            <p>mirror-intel uses S3-like storage backend,
            which means that you could not browse files like other mirror
            sites. Please follow our instructions to set up your software
            registry. If you intend to browse, we have provided an experimental
            API to browse bucket.</p>
            <p><a href="/{}/?mirror_intel_list">Browse {}</a></p>"#,
        route, route, route
    );
    resp.sized_body(body.len(), Cursor::new(body));
    resp.header(ContentType::HTML);
    resp.finalize()
}
