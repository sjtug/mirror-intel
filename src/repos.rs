use std::sync::LazyLock;

use actix_web::http::{Method, StatusCode, Uri};
use actix_web::{HttpRequest, HttpResponse, Route, Scope, guard, web};
use regex::Regex;

use crate::error::Result;
use crate::intel_path::IntelPath;
use crate::pypi_index;
use crate::{
    Error,
    common::{Config, Endpoints, IntelMission, IntelResponse, Redirect, Task},
    s3_cache::PreCacheStatus,
    utils,
};

/// Routing decision returned by a `classify` closure in [`simple_intel`].
pub enum RouteAction {
    /// Return 404 without consulting S3 or upstream.
    NotFound,
    /// Reverse-proxy GET from upstream and return an empty 200 response to HEAD.
    Proxy,
    /// Follow the smart-cache strategy (redirect HEAD, stream or cache GET).
    /// NOTE: Due to S3 API restriction, this is often paired with a
    /// prefetch cache to fix response code inconsistency.
    Cache,
    /// Permanently redirect (301) to the upstream URL.
    Redirect,
}

struct SimpleRequest {
    route: &'static str,
    origin: String,
    path: String,
    action: RouteAction,
    method: Method,
    uri: Uri,
}

async fn simple_intel_response(
    request: SimpleRequest,
    intel_mission: web::Data<IntelMission>,
    config: web::Data<Config>,
) -> Result<IntelResponse> {
    let task = Task {
        storage: request.route,
        retry_limit: config.max_retries,
        origin: request.origin,
        path: request.path,
    };

    // Rejected paths stay rejected even when a query string is present.
    if matches!(&request.action, RouteAction::NotFound) {
        return Ok(HttpResponse::NotFound().finish().into());
    }

    // Redirect (307) to upstream if any query param exists.
    // NOTE: use 302 instead of 307
    if let Some(query) = request.uri.query() {
        return Ok(Redirect::Temporary(format!("{}?{}", task.upstream_url(), query)).into());
    }

    match request.action {
        RouteAction::NotFound => unreachable!("handled before query-string redirect"),
        // Preserve legacy find-links behavior: GET proxies the upstream HTML while
        // HEAD reports availability without contacting upstream.
        RouteAction::Proxy => {
            let resp = if request.method == Method::HEAD {
                HttpResponse::Ok().finish().into()
            } else {
                task.resolve_upstream()
                    .reverse_proxy(&intel_mission)
                    .await?
                    .into()
            };
            Ok(resp)
        }
        // Smart-cache: consults the prefetch cache first. If no entry exists,
        // returns 404. HEAD requests redirect to origin; GET requests either stream
        // small cached objects directly or redirect for larger ones.
        RouteAction::Cache => {
            if matches!(
                intel_mission
                    .prefetch_cache
                    .prefetch_cache_action(&task, &intel_mission, &config)
                    .await,
                PreCacheStatus::None
            ) {
                return Ok(HttpResponse::NotFound().finish().into());
            }

            let resp = if request.method == Method::HEAD {
                task.resolve_no_content(&intel_mission, &config)
                    .await?
                    .redirect(&config)
                    .into()
            } else {
                task.resolve(&intel_mission, &config)
                    .await?
                    .stream_small_cached(config.direct_stream_size_kb, &intel_mission, &config)
                    .await?
            };
            Ok(resp)
        }
        // Permanent redirect (301) to the upstream URL.
        RouteAction::Redirect => Ok(Redirect::Permanent(task.upstream_url().to_string()).into()),
    }
}

pub fn simple_intel(
    origin_injection: impl Fn(&Endpoints) -> &str + Clone + Send + Sync + 'static,
    route: &'static str,
    classify: impl Fn(&Config, &str) -> RouteAction + Clone + Send + 'static,
) -> Route {
    let handler = move |path: IntelPath,
                        method: Method,
                        uri: Uri,
                        intel_mission: web::Data<IntelMission>,
                        config: web::Data<Config>| {
        let origin_injection = origin_injection.clone();
        let classify = classify.clone();
        async move {
            let origin = origin_injection(&config.endpoints).to_string();
            let path = path.to_string();
            let action = classify(&config, &path);
            simple_intel_response(
                SimpleRequest {
                    route,
                    origin,
                    path,
                    action,
                    method,
                    uri,
                },
                intel_mission,
                config,
            )
            .await
        }
    };
    // Route with handler for GET and HEAD methods, otherwise 404
    web::route()
        .guard(guard::Any(guard::Get()).or(guard::Head()))
        .to(handler)
}

/// Classify every path as [`RouteAction::Cache`].
///
/// Use as the `classify` argument for routes where all requests follow the
/// smart-cache strategy.
pub fn classify_cache_all(_config: &Config, _path: &str) -> RouteAction {
    RouteAction::Cache
}

/// Convert a boolean filter into a `classify` closure.
///
/// Paths that pass the filter get [`RouteAction::Cache`];
/// all others get [`RouteAction::Redirect`].
pub fn classify_with(
    filter: impl Fn(&Config, &str) -> bool + Clone + Send + 'static,
) -> impl Fn(&Config, &str) -> RouteAction + Clone + Send + 'static {
    move |config: &Config, path: &str| match filter(config, path) {
        true => RouteAction::Cache,
        false => RouteAction::Redirect,
    }
}

/// Classify paths for the pytorch-wheels route.
///
/// * Exact legacy `torch_stable.html` find-links page → [`RouteAction::Proxy`].
/// * Other legacy `.html` find-links pages → [`RouteAction::NotFound`].
/// * Source archives (`.tar.gz`, `.zip`, `.exe`) → [`RouteAction::Redirect`].
/// * Wheel artifacts and other non-index paths → [`RouteAction::Cache`].
///
/// Generated trailing-slash Simple Repository indexes are handled by the more
/// specific routes registered before this classifier.
pub fn wheels_route_classify(_config: &Config, path: &str) -> RouteAction {
    if path == "torch_stable.html" {
        return RouteAction::Proxy;
    }
    if path.ends_with(".html") {
        return RouteAction::NotFound;
    }

    // Source archives and Windows installers — redirect to upstream.
    if path.ends_with(".tar.gz") || path.ends_with(".zip") || path.ends_with(".exe") {
        return RouteAction::Redirect;
    }

    RouteAction::Cache
}

/// Mount a generated Simple Repository index and its on-demand artifacts.
///
/// Index documents are authoritative in `index_storage`; artifact requests use
/// the existing smart-cache classifier and may come from a different S3 prefix.
pub fn pypi_index_scope(
    public_route: &'static str,
    index_storage: &'static str,
    artifact_storage: &'static str,
    origin_injection: impl Fn(&Endpoints) -> &str + Clone + Send + Sync + 'static,
    classify: impl Fn(&Config, &str) -> RouteAction + Clone + Send + 'static,
) -> Scope {
    let root_handler = move |request: HttpRequest,
                             intel_mission: web::Data<IntelMission>,
                             config: web::Data<Config>| async move {
        if !request.path().ends_with('/') {
            let mut location = format!("/{public_route}/");
            if let Some(query) = request.uri().query() {
                location.push('?');
                location.push_str(query);
            }
            return Ok::<_, Error>(
                HttpResponse::MovedPermanently()
                    .insert_header(("Location", location))
                    .finish()
                    .into(),
            );
        }
        pypi_index::serve(
            index_storage,
            public_route,
            "",
            &request,
            intel_mission,
            config,
        )
        .await
    };
    let nested_handler = move |path: IntelPath,
                               request: HttpRequest,
                               intel_mission: web::Data<IntelMission>,
                               config: web::Data<Config>| async move {
        pypi_index::serve(
            index_storage,
            public_route,
            &path,
            &request,
            intel_mission,
            config,
        )
        .await
    };
    let slashless_origin = origin_injection.clone();
    let slashless_classify = classify.clone();
    let slashless_handler = move |path: IntelPath,
                                  method: Method,
                                  uri: Uri,
                                  intel_mission: web::Data<IntelMission>,
                                  config: web::Data<Config>| {
        let origin_injection = slashless_origin.clone();
        let classify = slashless_classify.clone();
        async move {
            let path = path.to_string();
            if let Some(canonical) =
                pypi_index::canonical_index_path(index_storage, &path, &intel_mission, &config)
                    .await?
            {
                let mut location = format!("/{public_route}/{canonical}/");
                if let Some(query) = uri.query() {
                    location.push('?');
                    location.push_str(query);
                }
                return Ok::<_, Error>(
                    HttpResponse::MovedPermanently()
                        .insert_header(("Location", location))
                        .finish()
                        .into(),
                );
            }

            let origin = origin_injection(&config.endpoints).to_string();
            let action = classify(&config, &path);
            simple_intel_response(
                SimpleRequest {
                    route: artifact_storage,
                    origin,
                    path,
                    action,
                    method,
                    uri,
                },
                intel_mission,
                config,
            )
            .await
        }
    };

    web::scope(&format!("/{public_route}"))
        .route(
            "",
            web::route()
                .guard(guard::Any(guard::Get()).or(guard::Head()))
                .to(root_handler),
        )
        .route(
            "/",
            web::route()
                .guard(guard::Any(guard::Get()).or(guard::Head()))
                .to(root_handler),
        )
        .route(
            "/{path:.+}/",
            web::route()
                .guard(guard::Any(guard::Get()).or(guard::Head()))
                .to(nested_handler),
        )
        .route(
            "/{path:(?:[^/]+/)*[^/.]+}",
            web::route()
                .guard(guard::Any(guard::Get()).or(guard::Head()))
                .to(slashless_handler),
        )
        .route(
            "/{path:.+}",
            simple_intel(origin_injection, artifact_storage, classify),
        )
}

pub fn ostree_allow(_config: &Config, path: &str) -> bool {
    !(path.starts_with("summary") || path.starts_with("config") || path.starts_with("refs/"))
}

pub fn rust_static_allow(_config: &Config, path: &str) -> bool {
    // ignore all folder other than `rustup`.
    if !path.starts_with("dist") && !path.starts_with("rustup") {
        return false;
    }

    // ignore `toml` under `rustup` folder
    if path.starts_with("rustup") && path.ends_with(".toml") {
        return false;
    }

    true
}

pub fn github_release_allow(config: &Config, path: &str) -> bool {
    static REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("^[^/]+/[^/]+/releases/download/[^/]+/[^/]+$").unwrap());

    if !config
        .github_release
        .allow
        .iter()
        .any(|repo| path.starts_with(repo))
    {
        return false;
    }

    REGEX.is_match(path)
}

pub fn sjtug_internal_allow(_config: &Config, path: &str) -> bool {
    static REGEX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new("^[^/]*/releases/download/[^/]*/[^/]*[.](tar[.]gz|zip)$").unwrap()
    });

    REGEX.is_match(path)
}

pub fn flutter_allow(_config: &Config, path: &str) -> bool {
    if path.starts_with("releases/") {
        return !path.ends_with(".json");
    }

    if path.starts_with("flutter/") {
        if path.ends_with("lcov.info") {
            return false;
        }
        return true;
    }

    if path.starts_with("android/") {
        return true;
    }

    if path.starts_with("gradle-wrapper/") {
        return true;
    }

    if path.starts_with("ios-usb-dependencies/") {
        return true;
    }

    if path.starts_with("mingit/") {
        return true;
    }

    false
}

pub fn linuxbrew_allow(_config: &Config, path: &str) -> bool {
    path.contains(".x86_64_linux")
}

pub fn gradle_allow(_config: &Config, path: &str) -> bool {
    path.ends_with(".zip")
}

pub fn configure_repo_routes(config: &mut web::ServiceConfig) {
    config
        .route(
            "/crates.io/{path:.+}",
            simple_intel(|c| &c.crates_io, "crates.io", classify_cache_all),
        )
        .route(
            "/flathub/{path:.+}",
            simple_intel(|c| &c.flathub, "flathub", classify_with(ostree_allow)),
        )
        .route(
            "/fedora-ostree/{path:.+}",
            simple_intel(
                |c| &c.fedora_ostree,
                "fedora-ostree",
                classify_with(ostree_allow),
            ),
        )
        .route(
            "/fedora-iot/{path:.+}",
            simple_intel(|c| &c.fedora_iot, "fedora-iot", classify_with(ostree_allow)),
        )
        .route(
            "/pypi-packages/{path:.+}",
            simple_intel(|c| &c.pypi_packages, "pypi-packages", classify_cache_all),
        )
        .route(
            "/homebrew-bottles/{path:.+}",
            simple_intel(
                |c| &c.homebrew_bottles,
                "homebrew-bottles",
                classify_cache_all,
            ),
        )
        .route(
            "/linuxbrew-bottles/{path:.+}",
            simple_intel(
                |c| &c.linuxbrew_bottles,
                "linuxbrew-bottles",
                classify_with(linuxbrew_allow),
            ),
        )
        .route(
            "/rust-static/{path:.+}",
            simple_intel(
                |c| &c.rust_static,
                "rust-static",
                classify_with(rust_static_allow),
            ),
        )
        .service(pypi_index_scope(
            "pytorch-wheels",
            "pytorch-wheels/simple",
            "pytorch-wheels",
            |c| &c.pytorch_wheels,
            wheels_route_classify,
        ))
        .service(pypi_index_scope(
            "astral-wheels",
            "astral-wheels/simple",
            "astral-wheels",
            |c| &c.astral_wheels,
            classify_cache_all,
        ))
        .route(
            "/sjtug-internal/{path:.+}",
            simple_intel(
                |c| &c.sjtug_internal,
                "sjtug-internal",
                classify_with(sjtug_internal_allow),
            ),
        )
        .route(
            "/flutter_infra/{path:.+}",
            simple_intel(
                |c| &c.flutter_infra,
                "flutter_infra",
                classify_with(flutter_allow),
            ),
        )
        .route(
            "/flutter_infra_release/{path:.+}",
            simple_intel(
                |c| &c.flutter_infra_release,
                "flutter_infra_release",
                classify_with(flutter_allow),
            ),
        )
        .route(
            "/github-release/{path:.+}",
            simple_intel(
                |c| &c.github_release,
                "github-release",
                classify_with(github_release_allow),
            ),
        )
        .route(
            "/opam-cache/{path:.+}",
            simple_intel(|c| &c.opam_cache, "opam-cache", classify_cache_all),
        )
        .route(
            "/gradle/distribution/{path:.+}",
            simple_intel(
                |c| &c.gradle_distribution,
                "gradle/distributions",
                classify_with(gradle_allow),
            ),
        )
        .route("/dart-pub/{path:.+}", web::get().to(dart_pub))
        .route("/pypi/web/simple/{path:.+}", web::get().to(pypi))
        .route("/guix/{path:.+}", nix_intel(|c| &c.guix, "guix"))
        .route(
            "/guix-bordeaux/{path:.+}",
            nix_intel(|c| &c.guix_bordeaux, "guix-bordeaux"),
        )
        .route(
            "/nix-channels/store/{path:.+}",
            nix_intel(|c| &c.nix_channels_store, "nix-channels/store"),
        );
}

pub async fn dart_pub(
    path: IntelPath,
    uri: Uri,
    intel_mission: web::Data<IntelMission>,
    config: web::Data<Config>,
) -> Result<IntelResponse> {
    let origin = config.endpoints.dart_pub.clone();
    let path = path.to_string();
    let task = Task {
        storage: "dart-pub",
        retry_limit: config.max_retries,
        origin: origin.clone(),
        path,
    };

    if let Some(query) = uri.query() {
        return Ok(Redirect::Temporary(format!("{}?{}", task.upstream_url(), query)).into());
    }

    if task.path.starts_with("api/") {
        Ok(task
            .resolve_upstream()
            .rewrite_upstream(
                &intel_mission,
                4096,
                |content| content.replace(&origin, &format!("{}/dart-pub", config.base_url)),
                &config,
            )
            .await?)
    } else if task.path.starts_with("packages/") {
        Ok(task
            .resolve(&intel_mission, &config)
            .await?
            .stream_small_cached(config.direct_stream_size_kb, &intel_mission, &config)
            .await?)
    } else {
        Ok(Redirect::Permanent(task.upstream_url().to_string()).into())
    }
}

pub async fn pypi(
    path: IntelPath,
    uri: Uri,
    intel_mission: web::Data<IntelMission>,
    config: web::Data<Config>,
) -> Result<IntelResponse> {
    let origin = config.endpoints.pypi_simple.clone();
    let path = path.to_string();
    let task = Task {
        storage: "pypi",
        retry_limit: config.max_retries,
        origin: origin.clone(),
        path,
    };

    if let Some(query) = uri.query() {
        return Ok(Redirect::Temporary(format!("{}?{}", task.upstream_url(), query)).into());
    }

    task.resolve_upstream()
        .rewrite_upstream(
            &intel_mission,
            4096,
            |content| {
                content.replace(
                    "../../packages",
                    &format!("{}/pypi-packages", config.base_url),
                )
            },
            &config,
        )
        .await
}

pub fn nix_intel(
    origin_injection: impl FnMut(&Endpoints) -> &str + Clone + Send + Sync + 'static,
    route: &'static str,
) -> Route {
    let handler = move |path: IntelPath,
                        uri: Uri,
                        intel_mission: web::Data<IntelMission>,
                        config: web::Data<Config>| {
        let mut origin_injection = origin_injection.clone();
        async move {
            let origin = origin_injection(&config.endpoints).to_string();
            let path = path.to_string();
            let task = Task {
                storage: route,
                retry_limit: config.max_retries,
                origin,
                path,
            };

            if let Some(query) = uri.query() {
                return Ok::<IntelResponse, Error>(
                    Redirect::Temporary(format!("{}?{}", task.upstream_url(), query)).into(),
                );
            }

            if task.path.starts_with("nar/") || task.path.ends_with(".narinfo") {
                match task
                    .resolve(&intel_mission, &config)
                    .await?
                    .reverse_proxy(&intel_mission)
                    .await
                {
                    Ok(resp) => Ok(resp.into()),
                    Err(Error::Http(status)) if status == StatusCode::NOT_FOUND => {
                        Ok(HttpResponse::NotFound().finish().into())
                    }
                    Err(Error::Reqwest(_)) => Ok(HttpResponse::NotFound().finish().into()),
                    Err(e) => Err(e),
                }
            } else if task.path == "nix-cache-info" {
                match task.resolve_upstream().reverse_proxy(&intel_mission).await {
                    Ok(resp) => Ok(resp.into()),
                    Err(Error::Http(status)) if status == StatusCode::NOT_FOUND => {
                        Ok(HttpResponse::NotFound().finish().into())
                    }
                    Err(Error::Reqwest(_)) => Ok(HttpResponse::NotFound().finish().into()),
                    Err(e) => Err(e),
                }
            } else {
                Ok(Redirect::Permanent(task.upstream_url().to_string()).into())
            }
        }
    };
    web::get().to(handler)
}

pub async fn index(path: IntelPath, config: web::Data<Config>) -> IntelResponse {
    if config
        .endpoints
        .s3_only
        .iter()
        .any(|x| path.starts_with(x) && &*path != x)
    {
        return Redirect::Permanent(format!(
            "{}/{}/{}",
            config.s3.website_endpoint, config.s3.bucket, path
        ))
        .into();
    }
    utils::no_route_for(&path).into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use actix_http::{Request, body};
    use actix_web::App;
    use actix_web::dev::{Service, ServiceResponse};
    use actix_web::http::StatusCode;
    use actix_web::test::{TestRequest, call_service, init_service};
    use figment::Figment;
    use figment::providers::{Format, Toml};
    use figment::util::map;
    use httpmock::MockServer;
    use reqwest::ClientBuilder;
    use rstest::rstest;
    use serial_test::serial;
    use tokio::sync::mpsc::{Receiver, channel};
    use url::Url;

    use crate::common::{Config, EndpointOverride, IntelMission, Metrics, S3Health};
    use crate::s3_cache::PrefetchCache;
    use crate::{list, not_found, queue_length, storage::get_anonymous_s3_client};

    use super::*;

    async fn make_service() -> (
        impl Service<Request, Response = ServiceResponse, Error = actix_web::Error>,
        Arc<Config>,
        Receiver<Task>,
        MockServer,
    ) {
        let server = MockServer::start_async().await;
        let _mock = server.mock_async(|when, then| {
            when
                .method(httpmock::Method::GET)
                .path("/bucket/sjtug-internal/mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz");
            then.status(200).body("ok");
        }).await;
        let _mock_2 = server.mock_async(|when, then| {
            when
                .method(httpmock::Method::HEAD)
                .path("/bucket/sjtug-internal/mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz");
            then.status(200).body("");
        }).await;
        let _mock_3 = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::HEAD)
                    .path("/mirror-clone/releases/download/v0.1.7/mirror-clone-2333.tar.gz");
                then.status(200).body("");
            })
            .await;
        let _mock_4 = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::HEAD)
                    .path("/mirror-clone/releases/download/v0.1.7/mirror%2B%2B%2B-clone.tar.gz");
                then.status(200).body("");
            })
            .await;
        let mut _index_mocks = vec![];
        for (method, path, body) in [
            (
                httpmock::Method::GET,
                "/bucket/pytorch-wheels/simple/index.v1_html",
                "<a href=\"torch/\">torch</a>",
            ),
            (
                httpmock::Method::GET,
                "/bucket/pytorch-wheels/simple/index.v1_json",
                r#"{"meta":{"api-version":"1.0"},"projects":[{"name":"torch"}]}"#,
            ),
            (
                httpmock::Method::GET,
                "/bucket/pytorch-wheels/simple/torch/index.v1_html",
                "<h1>Links for torch</h1>",
            ),
            (
                httpmock::Method::GET,
                "/bucket/pytorch-wheels/simple/torch/index.v1_json",
                r#"{"meta":{"api-version":"1.1"},"name":"torch","versions":[],"files":[]}"#,
            ),
            (
                httpmock::Method::HEAD,
                "/bucket/pytorch-wheels/simple/torch/index.v1_html",
                "",
            ),
            (
                httpmock::Method::HEAD,
                "/bucket/pytorch-wheels/simple/torch/index.v1_json",
                "",
            ),
            (
                httpmock::Method::HEAD,
                "/bucket/pytorch-wheels/simple/typing-extensions/index.v1_html",
                "",
            ),
            (
                httpmock::Method::GET,
                "/whl/torch_stable.html",
                "legacy torch find-links",
            ),
            (
                httpmock::Method::GET,
                "/bucket/astral-wheels/simple/cpu/index.v1_json",
                r#"{"meta":{"api-version":"1.0"},"projects":[{"name":"pyg-lib"}]}"#,
            ),
        ] {
            _index_mocks.push(
                server
                    .mock_async(move |when, then| {
                        when.method(method).path(path);
                        then.status(200).body(body);
                    })
                    .await,
            );
        }
        let sjtug_internal = server.base_url();
        let figment = Figment::new()
            .join(("address", "127.0.0.1"))
            .join(("port", 8000))
            .join(("concurrent_download", 512))
            .join(("max_pending_task", 16384))
            .join(("endpoints", map!["sjtug_internal" => sjtug_internal]))
            .join(("s3.name", "Placeholder S3"))
            .join(("s3.endpoint", server.base_url()))
            .join(("s3.website_endpoint", server.base_url()))
            .join(("s3.bucket", "bucket"))
            .join(("direct_stream_size_kb", 0))
            .merge(Toml::file(crate::common::rocket_toml_path()).nested());
        let mut config: Config = figment.extract().expect("config");
        config.read_only = true;
        config.endpoints.pytorch_wheels = format!("{}/whl", server.base_url());
        let config = Arc::new(config);

        let (tx, rx) = channel(1024);
        let client = ClientBuilder::new()
            .user_agent(&config.user_agent)
            .build()
            .unwrap();

        let mission = IntelMission {
            tx: Some(tx),
            client,
            prefetch_client: ClientBuilder::new()
                .user_agent(&config.user_agent)
                .build()
                .unwrap(),
            metrics: Arc::new(Metrics::default()),
            s3_health: S3Health::healthy(),
            s3_client: Arc::new(get_anonymous_s3_client(&config.s3)),
            prefetch_cache: Arc::new(PrefetchCache::new(Duration::from_secs(60))),
        };

        let app = App::new()
            .app_data(web::Data::new(mission.clone()))
            .app_data(web::Data::from(config.clone()))
            .route(
                "/{path:.+}",
                web::get()
                    .guard(guard::fn_guard(|ctx| {
                        ctx.head().uri.query() == Some("mirror_intel_list")
                    }))
                    .to(list),
            )
            .service(pypi_index_scope(
                "pytorch-wheels",
                "pytorch-wheels/simple",
                "pytorch-wheels",
                |c| &c.pytorch_wheels,
                wheels_route_classify,
            ))
            .service(pypi_index_scope(
                "astral-wheels",
                "astral-wheels/simple",
                "astral-wheels",
                |c| &c.astral_wheels,
                classify_cache_all,
            ))
            .route(
                "/sjtug-internal/{path:.+}",
                simple_intel(
                    |c| &c.sjtug_internal,
                    "sjtug-internal",
                    classify_with(sjtug_internal_allow),
                ),
            )
            .route("/{path:.+}", web::get().to(index))
            .default_service(web::route().to(not_found))
            .wrap_fn(queue_length);

        let service = init_service(app).await;

        (service, config, rx, server)
    }

    fn exist_object() -> Task {
        Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz".to_string(),
            retry_limit: 3,
        }
    }

    fn missing_object() -> Task {
        Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/mirror-clone-2333.tar.gz".to_string(),
            retry_limit: 3,
        }
    }

    fn forbidden_object() -> Task {
        Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/forbidden/mirror-clone.tar.gz".to_string(),
            retry_limit: 3,
        }
    }

    fn upstream_url(task: &Task, config: &Config) -> Url {
        Url::parse(&format!(
            "{}/{}",
            config.endpoints.sjtug_internal, task.path
        ))
        .expect("invalid test upstream url")
    }

    // NOTE: Set `#[serial(cwd_env)]` to avoid race condition between testcases
    #[rstest]
    #[case(
        Method::GET,
        exist_object(),
        StatusCode::MOVED_PERMANENTLY,
        Task::cached_url
    )]
    #[case(
        Method::HEAD,
        exist_object(),
        StatusCode::MOVED_PERMANENTLY,
        Task::cached_url
    )]
    #[case(Method::GET, missing_object(), StatusCode::FOUND, upstream_url)]
    #[case(Method::HEAD, missing_object(), StatusCode::FOUND, upstream_url)]
    #[case(
        Method::GET,
        forbidden_object(),
        StatusCode::MOVED_PERMANENTLY,
        upstream_url
    )]
    #[case(
        Method::HEAD,
        forbidden_object(),
        StatusCode::MOVED_PERMANENTLY,
        upstream_url
    )]
    #[serial(cwd_env)]
    #[tokio::test]
    async fn test_get_head(
        #[case] method: Method,
        #[case] object: Task,
        #[case] expected_status: StatusCode,
        #[case] expected_location_injection: impl FnOnce(&Task, &Config) -> Url,
    ) {
        // if an object is filtered, we should permanently redirect users to upstream
        let (service, config, _rx, _server) = make_service().await;
        let req = TestRequest::default()
            .method(method)
            .uri(object.root_path().as_str())
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), expected_status);
        assert_eq!(
            resp.headers().get("Location").unwrap().to_str().unwrap(),
            expected_location_injection(&object, &config).as_str()
        );
    }

    fn is_index_for(name: &str) -> impl FnOnce(&str) + '_ {
        move |resp| {
            // assert!(
            //     resp.contains(&format!("<title>Index of {}/</title>", name))
            // );
            assert!(!resp.contains(&format!("No route for {}.", name)));
        }
    }

    #[rstest]
    #[case("/pytorch-wheels/", is_index_for("pytorch-wheels"))]
    #[case("/pytorch-wheels/?mirror_intel_list", is_index_for("pytorch-wheels"))]
    #[case("/pytorch-wheels?mirror_intel_list", is_index_for("pytorch-wheels"))]
    #[serial(cwd_env)]
    #[tokio::test]
    async fn test_index_list_page(#[case] url: &str, #[case] assert_f: impl FnOnce(&str)) {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get().uri(url).to_request();
        let resp = call_service(&service, req).await;
        let body = body::to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert_f(text);
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn pytorch_root_negotiates_pep_691_json() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/")
            .insert_header((
                "Accept",
                "application/vnd.pypi.simple.v1+json, text/html;q=0.01",
            ))
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            "application/vnd.pypi.simple.v1+json"
        );
        assert_eq!(resp.headers().get("Vary").unwrap(), "Accept");
        assert_eq!(
            resp.headers().get("Cache-Control").unwrap(),
            "public, max-age=300"
        );
        let body = body::to_bytes(resp.into_body()).await.unwrap();
        let json = std::str::from_utf8(&body).unwrap();
        assert!(json.contains(r#""projects":[{"name":"torch"}]"#));
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn latest_json_request_returns_concrete_v1_content_type() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/")
            .insert_header(("Accept", "application/vnd.pypi.simple.latest+json"))
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            "application/vnd.pypi.simple.v1+json"
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn astral_channel_index_scope_serves_an_independent_repository() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/astral-wheels/cpu/")
            .insert_header(("Accept", "application/vnd.pypi.simple.v1+json"))
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body::to_bytes(resp.into_body()).await.unwrap();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains(r#""name":"pyg-lib""#)
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn pytorch_nested_project_serves_generated_html() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/torch/")
            .insert_header(("Accept", "application/vnd.pypi.simple.v1+html"))
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            "application/vnd.pypi.simple.v1+html; charset=utf-8"
        );
        let body = body::to_bytes(resp.into_body()).await.unwrap();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("Links for torch")
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn pytorch_project_name_redirects_to_normalized_name() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/Typing_Extensions/")
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            "/pytorch-wheels/typing-extensions/"
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn slashless_project_redirects_to_trailing_slash() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/torch?client=pip")
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            "/pytorch-wheels/torch/?client=pip"
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn slashless_project_redirects_to_normalized_name() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/Typing_Extensions")
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            "/pytorch-wheels/typing-extensions/"
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn pytorch_root_without_slash_redirects_to_canonical_url() {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get().uri("/pytorch-wheels").to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(resp.headers().get("Location").unwrap(), "/pytorch-wheels/");
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn test_url_segment() {
        // this case is to test if we could process escaped URL correctly
        let (service, config, _rx, _server) = make_service().await;
        let object = Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/mirror%2B%2B%2B-clone.tar.gz".to_string(),
            retry_limit: 3,
        };
        let req = TestRequest::default()
            .method(Method::HEAD)
            .uri(object.root_path().as_str())
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::FOUND);
        assert_eq!(
            resp.headers().get("Location").unwrap().to_str().unwrap(),
            upstream_url(&object, &config).as_str()
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn test_url_segment_fail() {
        // this case is to test if we could process escaped URL correctly
        let (service, _, _rx, _server) = make_service().await;
        let object = Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/.mirror%2B%2B%2B-clone.tar.gz".to_string(),
            retry_limit: 3,
        };
        let req = TestRequest::default()
            .method(Method::HEAD)
            .uri(object.root_path().as_str())
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn test_url_segment_query() {
        // this case is to test if we could process escaped URL correctly
        let (service, config, _rx, _server) = make_service().await;
        let object = Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path:
                "mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz?ci=233333&ci2=23333333"
                    .to_string(),
            retry_limit: 3,
        };
        let req = TestRequest::get()
            .uri(object.root_path().as_str())
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::FOUND);
        assert_eq!(
            resp.headers().get("Location").unwrap(),
            upstream_url(&object, &config).as_str()
        );
    }

    #[test]
    fn test_flutter_allow() {
        let config = Config::default();
        assert!(!flutter_allow(&config, "releases/releases_windows.json"));
        assert!(!flutter_allow(&config, "releases/releases_linux.json"));
        assert!(flutter_allow(
            &config,
            "releases/stable/linux/flutter_linux_1.17.0-stable.tar.xz",
        ));
        assert!(flutter_allow(
            &config,
            "flutter/069b3cf8f093d44ec4bae1319cbfdc4f8b4753b6/android-arm/artifacts.zip",
        ));
        assert!(flutter_allow(
            &config,
            "flutter/fonts/03bdd42a57aff5c496859f38d29825843d7fe68e/fonts.zip",
        ));
        assert!(!flutter_allow(&config, "flutter/coverage/lcov.info"));
    }

    #[test]
    fn test_wheels_route_classify() {
        let config = Config::default();
        // The exact historical find-links page keeps its reverse-proxy behavior.
        assert!(matches!(
            wheels_route_classify(&config, "torch_stable.html"),
            RouteAction::Proxy
        ));
        // Other legacy HTML pages are not dynamically proxied.
        assert!(matches!(
            wheels_route_classify(&config, "other.html"),
            RouteAction::NotFound
        ));
        // .whl files are cached
        assert!(matches!(
            wheels_route_classify(&config, "torch-2.0.0-cp311-cp311-manylinux.whl"),
            RouteAction::Cache
        ));
        // Invalid #sha256 fragment → Cache (smart cache handles unknown paths)
        assert!(matches!(
            wheels_route_classify(
                &config,
                "torch-2.0.0-cp311-cp311-manylinux.whl#sha256=0123456789abcdef"
            ),
            RouteAction::Cache
        ));
        // Valid .whl with hash → cache
        assert!(matches!(
            wheels_route_classify(
                &config,
                "cpu/torch-2.11.0%2Bcpu-cp314-cp314t-manylinux_2_28_x86_64.whl"
            ),
            RouteAction::Cache
        ));
        assert!(matches!(
            wheels_route_classify(
                &config,
                "cu130/torch-2.11.0%2Bcu130-cp314-cp314t-manylinux_2_28_x86_64.whl"
            ),
            RouteAction::Cache
        ));
        // Source archives and installers → redirect to upstream
        assert!(matches!(
            wheels_route_classify(&config, "torch-2.0.0.tar.gz"),
            RouteAction::Redirect
        ));
        assert!(matches!(
            wheels_route_classify(&config, "torch-2.0.0.zip"),
            RouteAction::Redirect
        ));
        assert!(matches!(
            wheels_route_classify(&config, "torch-2.0.0-cp311-cp311-win_amd64.exe"),
            RouteAction::Redirect
        ));
        // Unknown paths (directory indexes, etc.) → Cache (smart cache strategy)
        assert!(matches!(
            wheels_route_classify(&config, "torch"),
            RouteAction::Cache
        ));
        assert!(matches!(
            wheels_route_classify(&config, "cu130/torch"),
            RouteAction::Cache
        ));
    }

    #[test]
    fn test_task_override() {
        let mut task = Task {
            storage: "flutter_infra",
            retry_limit: 233,
            origin: "https://storage.flutter-io.cn/".to_string(),
            path: "test".to_string(),
        };
        task.apply_override(&[EndpointOverride {
            name: "flutter".to_string(),
            pattern: "https://storage.flutter-io.cn/".to_string(),
            replace: "https://storage.googleapis.com/".to_string(),
        }]);
        assert_eq!(task.origin, "https://storage.googleapis.com/");
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn legacy_torch_stable_get_reverse_proxies_upstream() {
        let (service, _, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/torch_stable.html")
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(body, "legacy torch find-links");
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn legacy_torch_stable_head_returns_ok() {
        let (service, _, _rx, _server) = make_service().await;
        let req = TestRequest::default()
            .method(Method::HEAD)
            .uri("/pytorch-wheels/torch_stable.html")
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.headers().contains_key("Location"));
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn legacy_torch_stable_query_redirects_upstream() {
        let (service, config, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/torch_stable.html?legacy=1")
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::FOUND);
        assert_eq!(
            resp.headers().get("Location").unwrap().to_str().unwrap(),
            format!(
                "{}/torch_stable.html?legacy=1",
                config.endpoints.pytorch_wheels
            )
        );
    }

    #[serial(cwd_env)]
    #[tokio::test]
    async fn other_legacy_html_remains_not_found_with_query() {
        let (service, _, _rx, _server) = make_service().await;
        let req = TestRequest::get()
            .uri("/pytorch-wheels/other.html?legacy=1")
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_github_release() {
        let mut config = Config::default();
        config.github_release.allow.push("sjtug/lug/".to_string());
        assert!(github_release_allow(
            &config,
            "sjtug/lug/releases/download/v0.0.0/test.txt",
        ));
        assert!(!github_release_allow(
            &config,
            "sjtug/lug/2333/releases/download/v0.0.0/test.txt",
        ));
        assert!(!github_release_allow(
            &config,
            "sjtug/lug2/releases/download/v0.0.0/test.txt",
        ));
    }
}
