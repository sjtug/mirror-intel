use actix_web::body::{BodyStream, SizedStream};
use actix_web::http::header::ContentType;
use actix_web::http::{header, StatusCode, Uri};
use actix_web::{HttpResponse, Responder};
use futures::stream::TryStreamExt;
use tracing::debug;
use url::Url;

use crate::common::Redirect;
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
            Self::Cached { .. } => Redirect::Permanent(self.target_url(config).to_string()),
            Self::Origin { .. } => Redirect::Temporary(self.target_url(config).to_string()),
        }
    }

    /// Respond with rewritten upstream response.
    pub async fn rewrite_upstream(
        self,
        intel_mission: &IntelMission,
        below_size_kb: u64, // only responses with size below this will be rewritten
        f: impl Fn(String) -> String,
        config: &Config,
    ) -> Result<IntelResponse> {
        let task = self.task();
        let upstream = task.upstream_url();
        let resp = intel_mission.client.get(upstream).send().await?;

        if resp.status().is_success() {
            if let Some(content_length) = resp.content_length() {
                if content_length <= below_size_kb * 1024 {
                    let text = resp.text().await?;
                    let text = f(text);
                    return Ok(HttpResponse::Ok()
                        .content_type(ContentType::octet_stream())
                        .body(text)
                        .into());
                }
            }
        }

        Ok(self.redirect(config).into())
    }

    /// Respond with reverse proxy.
    pub async fn reverse_proxy(self, intel_mission: &IntelMission) -> Result<HttpResponse> {
        let upstream_resp = match self {
            Self::Cached { resp, .. } => resp,
            Self::Origin { ref task } => {
                intel_mission.client.get(task.upstream_url()).send().await?
            }
        };

        let code = upstream_resp.status().normalize();
        let mut resp = HttpResponse::build(code);
        if let Some(content_type) = upstream_resp.headers().get(header::CONTENT_TYPE) {
            resp.content_type(content_type);
        }
        let content_length = upstream_resp.content_length();
        let stream = upstream_resp
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e));

        Ok(if let Some(size) = content_length {
            resp.body(SizedStream::new(size, stream))
        } else {
            resp.body(BodyStream::new(stream))
        })
    }

    /// Respond with reverse proxy if it's a small cached file, or redirect otherwise.
    pub async fn stream_small_cached(
        self,
        size_kb: u64,
        intel_mission: &IntelMission,
        config: &Config,
    ) -> Result<IntelResponse> {
        match &self {
            Self::Cached { resp, .. } => {
                if let Some(content_length) = resp.content_length() {
                    if content_length <= size_kb * 1024 {
                        debug!("{} <= {}, direct stream", content_length, size_kb * 1024);
                        return Ok(self.reverse_proxy(intel_mission).await?.into());
                    }
                }
                Ok(self.redirect(config).into())
            }
            Self::Origin { .. } => Ok(self.redirect(config).into()),
        }
    }
}

/// 404 page.
#[allow(clippy::unused_async)]
pub async fn not_found(uri: Uri) -> impl Responder {
    no_route_for(&uri.to_string())
}

/// No route page.
///
/// Hint user to redirect to the S3 index page.
pub fn no_route_for(mut route: &str) -> HttpResponse {
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
    HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(body)
}

trait StatusCodeExt {
    /// Deal with special case for NGINX 499.
    fn normalize(self) -> StatusCode;
}

impl StatusCodeExt for StatusCode {
    fn normalize(self) -> StatusCode {
        if self.as_u16() == 499 {
            Self::NOT_FOUND
        } else {
            self
        }
    }
}
