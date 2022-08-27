use std::io::Cursor;

use futures::stream::TryStreamExt;
use response::ResponseBuilder;
use rocket::{
    http::ContentType,
    response::{self, Redirect},
    Response,
};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;

use crate::common::{Config, IntelMission, IntelObject, IntelResponse, Task};
use crate::error::{Error, Result};

impl Task {
    /// Resolve a task.
    ///
    /// Returns cache if it is available, otherwise schedules a download and returns origin.
    async fn resolve_internal(
        self,
        mission: &IntelMission,
        config: &Config,
        head: bool,
    ) -> Result<IntelObject> {
        mission.metrics.resolve_counter.inc();
        let req = if head {
            mission.client.head(self.cached_url(config))
        } else {
            mission.client.get(self.cached_url(config))
        };
        match req.send().await {
            Ok(resp) if resp.status().is_success() => Ok(IntelObject::Cached { task: self, resp }),
            _ => {
                if let Some(tx) = &mission.tx {
                    mission.metrics.task_in_queue.inc();
                    // TODO this may block if the queue is full, which is not good
                    tx.clone()
                        .send(self.clone())
                        .await
                        .map_err(|_| Error::SendError(()))?;
                }
                Ok(IntelObject::Origin { task: self })
            }
        }
    }

    /// Resolve a task.
    ///
    /// Returns cache if it is available, otherwise schedules a download and returns origin.
    ///
    /// # Content
    ///
    /// This method returns content of the cache if it is available.
    /// If you don't need it, consider using `resolve_no_content` instead.
    pub async fn resolve(self, mission: &IntelMission, config: &Config) -> Result<IntelObject> {
        self.resolve_internal(mission, config, false).await
    }

    /// Resolve a task.
    ///
    /// Returns cache if it is available, otherwise schedules a download and returns origin.
    ///
    /// # Content
    ///
    /// This method doesn't return content of the cache if it is available.
    /// If you need it, consider using `resolve` instead.
    pub async fn resolve_no_content(
        self,
        mission: &IntelMission,
        config: &Config,
    ) -> Result<IntelObject> {
        self.resolve_internal(mission, config, true).await
    }

    /// Always resolves to upstream. Neither returns cache nor schedules a download.
    pub const fn resolve_upstream(self) -> IntelObject {
        IntelObject::Origin { task: self }
    }
}

impl IntelObject {
    /// Extract original task from the object.
    pub const fn task(&self) -> &Task {
        match self {
            Self::Cached { task, .. } | Self::Origin { task, .. } => task,
        }
    }

    /// Get URL target.
    pub fn target_url(&self, config: &Config) -> Url {
        match self {
            Self::Cached { task, .. } => task.cached_url(config),
            Self::Origin { task } => task.upstream_url(),
        }
    }

    /// Respond with redirection.
    pub fn redirect(self, config: &Config) -> Redirect {
        match &self {
            Self::Cached { .. } => Redirect::moved(self.target_url(config).to_string()),
            Self::Origin { .. } => Redirect::found(self.target_url(config).to_string()),
        }
    }

    /// Respond with rewritten upstream response.
    pub async fn rewrite_upstream(
        self,
        intel_mission: &IntelMission,
        below_size_kb: u64, // only responses with size below this will be rewritten
        f: impl Fn(String) -> String,
        config: &Config,
    ) -> Result<IntelResponse<'static>> {
        let task = self.task();
        let upstream = task.upstream_url();
        let resp = intel_mission.client.get(upstream).send().await?;

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

    /// Pass status code from upstream to new response.
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

    /// Respond with reverse proxy.
    pub async fn reverse_proxy(
        self,
        intel_mission: &IntelMission,
    ) -> Result<IntelResponse<'static>> {
        match self {
            Self::Cached { resp, .. } => {
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
            Self::Origin { ref task } => {
                let resp = intel_mission.client.get(task.upstream_url()).send().await?;
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

    /// Respond with reverse proxy if it's a small cached file, or redirect otherwise.
    pub async fn stream_small_cached(
        self,
        size_kb: u64,
        intel_mission: &IntelMission,
        config: &Config,
    ) -> Result<IntelResponse<'static>> {
        match &self {
            Self::Cached { resp, .. } => {
                if let Some(content_length) = resp.content_length() {
                    if content_length <= size_kb * 1024 {
                        debug!("{} <= {}, direct stream", content_length, size_kb * 1024);
                        return self.reverse_proxy(intel_mission).await;
                    }
                }
                Ok(self.redirect(config).into())
            }
            Self::Origin { .. } => Ok(self.redirect(config).into()),
        }
    }
}

/// 404 page.
#[catch(404)]
pub fn not_found(req: &rocket::Request) -> Response<'static> {
    no_route_for(&req.uri().to_string())
}

/// No route page.
///
/// Hint user to redirect to the S3 index page.
pub fn no_route_for(mut route: &str) -> Response<'static> {
    let mut resp = Response::build();
    if route.ends_with('/') {
        route = &route[..route.len() - 1];
    }
    if route.starts_with('/') {
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
