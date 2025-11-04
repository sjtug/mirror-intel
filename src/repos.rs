use actix_web::http::{Method, Uri};
use actix_web::{guard, web, HttpResponse, Route};
use lazy_static::lazy_static;
use regex::Regex;

use crate::error::Result;
use crate::intel_path::IntelPath;
use crate::{
    common::{Config, Endpoints, IntelMission, IntelResponse, Redirect, Task},
    utils, Error,
};

pub fn simple_intel(
    origin_injection: impl FnMut(&Endpoints) -> &str + Clone + Send + Sync + 'static,
    route: &'static str,
    filter: impl FnMut(&Config, &str) -> bool + Clone + Send + 'static,
    proxy: impl FnMut(&str) -> bool + Clone + Send + 'static,
) -> Route {
    let handler = move |path: IntelPath,
                        method: Method,
                        uri: Uri,
                        intel_mission: web::Data<IntelMission>,
                        config: web::Data<Config>| {
        let mut origin_injection = origin_injection.clone();
        let mut filter = filter.clone();
        let mut proxy = proxy.clone();
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
                return Ok::<_, Error>(
                    Redirect::Temporary(format!("{}?{}", task.upstream_url(), query)).into(),
                );
            }

            if !filter(&config, &task.path) {
                return Ok(Redirect::Permanent(task.upstream_url().to_string()).into());
            }

            if proxy(&task.path) {
                return Ok(if method == Method::HEAD {
                    HttpResponse::Ok().finish().into()
                } else {
                    task.resolve_upstream()
                        .reverse_proxy(&intel_mission)
                        .await?
                        .into()
                });
            }

            Ok(if method == Method::HEAD {
                task.resolve_no_content(&intel_mission, &config)
                    .await?
                    .redirect(&config)
                    .into()
            } else {
                task.resolve(&intel_mission, &config)
                    .await?
                    .stream_small_cached(config.direct_stream_size_kb, &intel_mission, &config)
                    .await?
            })
        }
    };
    web::route()
        .guard(guard::Any(guard::Get()).or(guard::Head()))
        .to(handler)
}

pub const fn disallow_all(_path: &str) -> bool {
    false
}

pub const fn allow_all(_config: &Config, _path: &str) -> bool {
    true
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

pub fn wheels_allow(_config: &Config, path: &str) -> bool {
    path.ends_with(".whl") || path.ends_with(".html")
}

pub fn github_release_allow(config: &Config, path: &str) -> bool {
    lazy_static! {
        static ref REGEX: Regex =
            Regex::new("^[^/]*/[^/]*/releases/download/[^/]*/[^/]*$").unwrap();
    };

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
    lazy_static! {
        static ref REGEX: Regex =
            Regex::new("^[^/]*/releases/download/[^/]*/[^/]*.(tar.gz|zip)$").unwrap();
    };

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

pub fn wheels_proxy(path: &str) -> bool {
    path.ends_with(".html")
}

pub fn gradle_allow(_config: &Config, path: &str) -> bool {
    path.ends_with(".zip")
}

pub fn configure_repo_routes(config: &mut web::ServiceConfig) {
    config
        .route(
            "/crates.io/{path:.+}",
            simple_intel(|c| &c.crates_io, "crates.io", allow_all, disallow_all),
        )
        .route(
            "/flathub/{path:.+}",
            simple_intel(|c| &c.flathub, "flathub", ostree_allow, disallow_all),
        )
        .route(
            "/fedora-ostree/{path:.+}",
            simple_intel(
                |c| &c.fedora_ostree,
                "fedora-ostree",
                ostree_allow,
                disallow_all,
            ),
        )
        .route(
            "/fedora-iot/{path:.+}",
            simple_intel(|c| &c.fedora_iot, "fedora-iot", ostree_allow, disallow_all),
        )
        .route(
            "/pypi-packages/{path:.+}",
            simple_intel(
                |c| &c.pypi_packages,
                "pypi-packages",
                allow_all,
                disallow_all,
            ),
        )
        .route(
            "/homebrew-bottles/{path:.+}",
            simple_intel(
                |c| &c.homebrew_bottles,
                "homebrew-bottles",
                allow_all,
                disallow_all,
            ),
        )
        .route(
            "/linuxbrew-bottles/{path:.+}",
            simple_intel(
                |c| &c.linuxbrew_bottles,
                "linuxbrew-bottles",
                linuxbrew_allow,
                disallow_all,
            ),
        )
        .route(
            "/rust-static/{path:.+}",
            simple_intel(
                |c| &c.rust_static,
                "rust-static",
                rust_static_allow,
                disallow_all,
            ),
        )
        .route(
            "/pytorch-wheels/{path:.+}",
            simple_intel(
                |c| &c.pytorch_wheels,
                "pytorch-wheels",
                wheels_allow,
                wheels_proxy,
            ),
        )
        .route(
            "/sjtug-internal/{path:.+}",
            simple_intel(
                |c| &c.sjtug_internal,
                "sjtug-internal",
                sjtug_internal_allow,
                disallow_all,
            ),
        )
        .route(
            "/flutter_infra/{path:.+}",
            simple_intel(
                |c| &c.flutter_infra,
                "flutter_infra",
                flutter_allow,
                disallow_all,
            ),
        )
        .route(
            "/flutter_infra_release/{path:.+}",
            simple_intel(
                |c| &c.flutter_infra_release,
                "flutter_infra_release",
                flutter_allow,
                disallow_all,
            ),
        )
        .route(
            "/github-release/{path:.+}",
            simple_intel(
                |c| &c.github_release,
                "github-release",
                github_release_allow,
                disallow_all,
            ),
        )
        .route(
            "/opam-cache/{path:.+}",
            simple_intel(|c| &c.opam_cache, "opam-cache", allow_all, disallow_all),
        )
        .route(
            "/gradle/distribution/{path:.+}",
            simple_intel(
                |c| &c.gradle_distribution,
                "gradle/distributions",
                gradle_allow,
                disallow_all,
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
                Ok(task
                    .resolve(&intel_mission, &config)
                    .await?
                    .reverse_proxy(&intel_mission)
                    .await?
                    .into())
            } else if task.path == "nix-cache-info" {
                Ok(task
                    .resolve_upstream()
                    .reverse_proxy(&intel_mission)
                    .await?
                    .into())
            } else {
                Ok(Redirect::Permanent(task.upstream_url().to_string()).into())
            }
        }
    };
    web::get().to(handler)
}

#[allow(clippy::unused_async)]
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

    use actix_http::{body, Request};
    use actix_web::dev::{Service, ServiceResponse};
    use actix_web::http::StatusCode;
    use actix_web::test::{call_service, init_service, TestRequest};
    use actix_web::App;
    use figment::providers::{Format, Toml};
    use figment::Figment;
    use httpmock::MockServer;
    use reqwest::ClientBuilder;
    use rstest::rstest;
    use tokio::sync::mpsc::{channel, Receiver};
    use url::Url;

    use crate::common::EndpointOverride;
    use crate::{
        common::{Config, IntelMission, Metrics},
        list, not_found, queue_length,
        storage::get_anonymous_s3_client,
    };

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
        let figment = Figment::new()
            .join(("address", "127.0.0.1"))
            .join(("port", 8000))
            .join(("s3.website_endpoint", server.base_url()))
            .join(("s3.bucket", "bucket"))
            .join(("direct_stream_size_kb", 0))
            .merge(Toml::file("Rocket.toml").nested());
        let mut config: Config = figment.extract().expect("config");
        config.read_only = true;
        let config = Arc::new(config);

        let (tx, rx) = channel(1024);
        let client = ClientBuilder::new()
            .user_agent(&config.user_agent)
            .build()
            .unwrap();

        let mission = IntelMission {
            tx: Some(tx),
            client,
            metrics: Arc::new(Metrics::default()),
            s3_client: Arc::new(get_anonymous_s3_client(&config.s3)),
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
            .route(
                "/pytorch-wheels/{path:.+}",
                simple_intel(
                    |c| &c.pytorch_wheels,
                    "pytorch-wheels",
                    wheels_allow,
                    wheels_proxy,
                ),
            )
            .route(
                "/sjtug-internal/{path:.+}",
                simple_intel(
                    |c| &c.sjtug_internal,
                    "sjtug-internal",
                    sjtug_internal_allow,
                    disallow_all,
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
    #[case(Method::GET, missing_object(), StatusCode::FOUND, | o: & Task, _c: & Config | o.upstream_url())]
    #[case(Method::HEAD, missing_object(), StatusCode::FOUND, | o: & Task, _c: & Config | o.upstream_url())]
    #[case(Method::GET, forbidden_object(), StatusCode::MOVED_PERMANENTLY, | o: & Task, _c: & Config | o.upstream_url())]
    #[case(Method::HEAD, forbidden_object(), StatusCode::MOVED_PERMANENTLY, | o: & Task, _c: & Config | o.upstream_url())]
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
            expected_location_injection(&object, &*config).as_str()
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

    fn is_no_route_for(name: &str) -> impl FnOnce(&str) + '_ {
        move |resp| {
            assert!(resp.contains(&format!("No route for {}.", name)));
        }
    }

    #[rstest]
    #[case("/pytorch-wheels/", is_no_route_for("pytorch-wheels"))]
    #[case("/pytorch-wheels", is_no_route_for("pytorch-wheels"))]
    #[case("/pytorch-wheels/?mirror_intel_list", is_index_for("pytorch-wheels"))]
    #[case("/pytorch-wheels?mirror_intel_list", is_index_for("pytorch-wheels"))]
    #[tokio::test]
    async fn test_index_list_page(#[case] url: &str, #[case] assert_f: impl FnOnce(&str)) {
        let (service, _config, _rx, _server) = make_service().await;
        let req = TestRequest::get().uri(url).to_request();
        let resp = call_service(&service, req).await;
        let body = body::to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&*body).unwrap();
        assert_f(text);
    }

    #[tokio::test]
    async fn test_url_segment() {
        // this case is to test if we could process escaped URL correctly
        let (service, _, _rx, _server) = make_service().await;
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
            object.upstream_url().as_str()
        );
    }

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

    #[tokio::test]
    async fn test_url_segment_query() {
        // this case is to test if we could process escaped URL correctly
        let (service, _, _rx, _server) = make_service().await;
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
            object.upstream_url().as_str()
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

    #[tokio::test]
    async fn test_proxy_head() {
        // if an object doesn't exist in s3, we should temporarily redirect users to upstream
        let (service, _, _rx, _server) = make_service().await;
        let object = Task {
            storage: "pytorch-wheels",
            origin: "https://download.pytorch.org/whl".to_string(),
            path: "torch_stable.html".to_string(),
            retry_limit: 3,
        };
        let req = TestRequest::default()
            .method(Method::HEAD)
            .uri(object.root_path().as_str())
            .to_request();
        let resp = call_service(&service, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
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
