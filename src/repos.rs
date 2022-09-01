use actix_web::http::{Method, Uri};
use actix_web::{guard, web, HttpResponse, Route, Scope};
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

pub fn repo_routes() -> Scope {
    web::scope("")
        .route(
            "/crates-io/{path:.*}",
            simple_intel(|c| &c.crates_io, "crates_io", allow_all, disallow_all),
        )
        .route(
            "/flathub/{path:.*}",
            simple_intel(|c| &c.flathub, "flathub", ostree_allow, disallow_all),
        )
        .route(
            "/fedora-ostree/{path:.*}",
            simple_intel(
                |c| &c.fedora_ostree,
                "fedora-ostree",
                ostree_allow,
                disallow_all,
            ),
        )
        .route(
            "/fedora-iot/{path:.*}",
            simple_intel(|c| &c.fedora_iot, "fedora-iot", ostree_allow, disallow_all),
        )
        .route(
            "/pypi-packages/{path:.*}",
            simple_intel(
                |c| &c.pypi_packages,
                "pypi-packages",
                allow_all,
                disallow_all,
            ),
        )
        .route(
            "/homebrew-bottles/{path:.*}",
            simple_intel(
                |c| &c.homebrew_bottles,
                "homebrew-bottles",
                allow_all,
                disallow_all,
            ),
        )
        .route(
            "/linuxbrew-bottles/{path:.*}",
            simple_intel(
                |c| &c.linuxbrew_bottles,
                "linuxbrew-bottles",
                linuxbrew_allow,
                disallow_all,
            ),
        )
        .route(
            "/rust-static/{path:.*}",
            simple_intel(
                |c| &c.rust_static,
                "rust-static",
                rust_static_allow,
                disallow_all,
            ),
        )
        .route(
            "/pytorch-wheels/{path:.*}",
            simple_intel(
                |c| &c.pytorch_wheels,
                "pytorch-wheels",
                wheels_allow,
                wheels_proxy,
            ),
        )
        .route(
            "/sjtug-internal/{path:.*}",
            simple_intel(
                |c| &c.sjtug_internal,
                "sjtug-internal",
                sjtug_internal_allow,
                disallow_all,
            ),
        )
        .route(
            "/flutter_infra/{path:.*}",
            simple_intel(
                |c| &c.flutter_infra,
                "flutter_infra",
                flutter_allow,
                disallow_all,
            ),
        )
        .route(
            "/flutter_infra_release/{path:.*}",
            simple_intel(
                |c| &c.flutter_infra_release,
                "flutter_infra_release",
                flutter_allow,
                disallow_all,
            ),
        )
        .route(
            "/github-release/{path:.*}",
            simple_intel(
                |c| &c.github_release,
                "github-release",
                github_release_allow,
                disallow_all,
            ),
        )
        .route(
            "/opam-cache/{path:.*}",
            simple_intel(|c| &c.opam_cache, "opam-cache", allow_all, disallow_all),
        )
        .route(
            "/gradle/distribution/{path:.*}",
            simple_intel(
                |c| &c.gradle_distribution,
                "gradle/distributions",
                gradle_allow,
                disallow_all,
            ),
        )
        .route("/dart-pub/{path:.*}", web::get().to(dart_pub))
        .route("/pypi/web/simple/{path:.*}", web::get().to(pypi))
        .route("/guix/{path:.*}", nix_intel(|c| &c.guix, "guix"))
        .route(
            "/nix-channels/store/{path:.*}",
            nix_intel(|c| &c.nix_channels_store, "guix"),
        )
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

            if task.path.starts_with("nar/") {
                Ok(task
                    .resolve(&intel_mission, &config)
                    .await?
                    .reverse_proxy(&intel_mission)
                    .await?
                    .into())
            } else if task.path.ends_with(".narinfo") {
                Ok(task
                    .resolve(&intel_mission, &config)
                    .await?
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

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//
//     use reqwest::ClientBuilder;
//     use rocket::http::Status;
//     use tokio::sync::mpsc::{channel, Receiver};
//
//     use crate::queue::QueueLength;
//     use crate::utils::not_found;
//     use crate::{
//         common::{Config, EndpointOverride, IntelMission, Metrics},
//         storage::get_anonymous_s3_client,
//     };
//
//     use super::*;
//
//     async fn make_rocket() -> (rocket::local::asynchronous::Client, Config, Receiver<Task>) {
//         use rocket::local::asynchronous::Client;
//         let rocket = rocket::ignite();
//         let figment = rocket.figment();
//         let mut config: Config = figment.extract().expect("config");
//         config.read_only = true;
//
//         let (tx, rx) = channel(1024);
//         let client = ClientBuilder::new()
//             .user_agent(&config.user_agent)
//             .build()
//             .unwrap();
//
//         let mission = IntelMission {
//             tx: Some(tx),
//             client,
//             metrics: Arc::new(Metrics::default()),
//             s3_client: Arc::new(get_anonymous_s3_client(&config.s3)),
//         };
//
//         let queue_length_fairing = QueueLength {
//             mission: mission.clone(),
//         };
//
//         let rocket = rocket
//             .manage(mission)
//             .manage(config.clone())
//             .attach(queue_length_fairing)
//             .register(catchers![not_found])
//             .mount(
//                 "/",
//                 routes![
//                      sjtug_internal_head,
//                      sjtug_internal_get,
//                      pytorch_wheels_head,
//                      pytorch_wheels_get
//                  ],
//             );
//
//         (
//             Client::tracked(rocket)
//                 .await
//                 .expect("valid rocket instance"),
//             config,
//             rx,
//         )
//     }
//
//     fn exist_object() -> Task {
//         Task {
//             storage: "sjtug-internal",
//             origin: "https://github.com/sjtug".to_string(),
//             path: "mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz".to_string(),
//             retry_limit: 3,
//         }
//     }
//
//     fn missing_object() -> Task {
//         Task {
//             storage: "sjtug-internal",
//             origin: "https://github.com/sjtug".to_string(),
//             path: "mirror-clone/releases/download/v0.1.7/mirror-clone-2333.tar.gz".to_string(),
//             retry_limit: 3,
//         }
//     }
//
//     fn forbidden_object() -> Task {
//         Task {
//             storage: "sjtug-internal",
//             origin: "https://github.com/sjtug".to_string(),
//             path: "mirror-clone/releases/download/v0.1.7/forbidden/mirror-clone.tar.gz".to_string(),
//             retry_limit: 3,
//         }
//     }
//
//     #[rocket::async_test]
//     async fn test_redirect_exist_get() {
//         // if an object exists in s3, we should permanently redirect users to s3
//         let (client, config, _rx) = make_rocket().await;
//         let object = exist_object();
//         let response = client.get(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::MovedPermanently);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.cached_url(&config).as_str()]
//         );
//     }
//
//     #[rocket::async_test]
//     async fn test_redirect_exist_head() {
//         let (client, config, _rx) = make_rocket().await;
//         let object = exist_object();
//         let response = client.head(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::MovedPermanently);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.cached_url(&config).as_str()]
//         );
//     }
//
//     #[rocket::async_test]
//     async fn test_redirect_missing_get() {
//         // if an object doesn't exist in s3, we should temporarily redirect users to upstream
//         let (client, _, _rx) = make_rocket().await;
//         let object = missing_object();
//         let response = client.get(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::Found);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.upstream_url().as_str()]
//         );
//     }
//
//     #[rocket::async_test]
//     async fn test_redirect_missing_head() {
//         // if an object doesn't exist in s3, we should temporarily redirect users to upstream
//         let (client, _, _rx) = make_rocket().await;
//         let object = missing_object();
//         let response = client.head(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::Found);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.upstream_url().as_str()]
//         );
//     }
//
//     #[rocket::async_test]
//     async fn test_redirect_forbidden_get() {
//         // if an object is filtered, we should permanently redirect users to upstream
//         let (client, _, _rx) = make_rocket().await;
//         let object = forbidden_object();
//         let response = client.get(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::MovedPermanently);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.upstream_url().as_str()]
//         );
//     }
//
//     #[rocket::async_test]
//     async fn test_redirect_forbidden_head() {
//         // if an object is filtered, we should permanently redirect users to upstream
//         let (client, _, _rx) = make_rocket().await;
//         let object = forbidden_object();
//         let response = client.head(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::MovedPermanently);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.upstream_url().as_str()]
//         );
//     }
//
//     #[rocket::async_test]
//     async fn test_url_segment() {
//         // this case is to test if we could process escaped URL correctly
//         let (client, _, _rx) = make_rocket().await;
//         let object = Task {
//             storage: "sjtug-internal",
//             origin: "https://github.com/sjtug".to_string(),
//             path: "mirror-clone/releases/download/v0.1.7/mirror%2B%2B%2B-clone.tar.gz".to_string(),
//             retry_limit: 3,
//         };
//         let response = client.head(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::Found);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.upstream_url().as_str()]
//         );
//     }
//
//     #[rocket::async_test]
//     async fn test_url_segment_fail() {
//         // this case is to test if we could process escaped URL correctly
//         let (client, _, _rx) = make_rocket().await;
//         let object = Task {
//             storage: "sjtug-internal",
//             origin: "https://github.com/sjtug".to_string(),
//             path: "mirror-clone/releases/download/v0.1.7/.mirror%2B%2B%2B-clone.tar.gz".to_string(),
//             retry_limit: 3,
//         };
//         let response = client.head(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::NotFound);
//     }
//
//     #[rocket::async_test]
//     async fn test_url_segment_query() {
//         // this case is to test if we could process escaped URL correctly
//         let (client, _, _rx) = make_rocket().await;
//         let object = Task {
//             storage: "sjtug-internal",
//             origin: "https://github.com/sjtug".to_string(),
//             path:
//             "mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz?ci=233333&ci2=23333333"
//                 .to_string(),
//             retry_limit: 3,
//         };
//         let response = client.get(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::Found);
//         assert_eq!(
//             response.headers().get("Location").collect::<Vec<&str>>(),
//             vec![object.upstream_url().as_str()]
//         );
//     }
//
//     #[test]
//     fn test_flutter_allow() {
//         let config = Config::default();
//         assert!(!flutter_allow(&config, "releases/releases_windows.json"));
//         assert!(!flutter_allow(&config, "releases/releases_linux.json"));
//         assert!(flutter_allow(
//             &config,
//             "releases/stable/linux/flutter_linux_1.17.0-stable.tar.xz",
//         ));
//         assert!(flutter_allow(
//             &config,
//             "flutter/069b3cf8f093d44ec4bae1319cbfdc4f8b4753b6/android-arm/artifacts.zip",
//         ));
//         assert!(flutter_allow(
//             &config,
//             "flutter/fonts/03bdd42a57aff5c496859f38d29825843d7fe68e/fonts.zip",
//         ));
//         assert!(!flutter_allow(&config, "flutter/coverage/lcov.info"));
//     }
//
//     #[test]
//     fn test_task_override() {
//         let mut task = Task {
//             storage: "flutter_infra",
//             retry_limit: 233,
//             origin: "https://storage.flutter-io.cn/".to_string(),
//             path: "test".to_string(),
//         };
//         task.apply_override(&[EndpointOverride {
//             name: "flutter".to_string(),
//             pattern: "https://storage.flutter-io.cn/".to_string(),
//             replace: "https://storage.googleapis.com/".to_string(),
//         }]);
//         assert_eq!(task.origin, "https://storage.googleapis.com/");
//     }
//
//     #[rocket::async_test]
//     async fn test_proxy_head() {
//         // if an object doesn't exist in s3, we should temporarily redirect users to upstream
//         let (client, _, _rx) = make_rocket().await;
//         let object = Task {
//             storage: "pytorch-wheels",
//             origin: "https://download.pytorch.org/whl".to_string(),
//             path: "torch_stable.html".to_string(),
//             retry_limit: 3,
//         };
//         let response = client.head(object.root_path()).dispatch().await;
//         assert_eq!(response.status(), Status::Ok);
//     }
//
//     #[rocket::async_test]
//     async fn test_github_release() {
//         let mut config = Config::default();
//         config.github_release.allow.push("sjtug/lug/".to_string());
//         assert!(github_release_allow(
//             &config,
//             "sjtug/lug/releases/download/v0.0.0/test.txt",
//         ));
//         assert!(!github_release_allow(
//             &config,
//             "sjtug/lug/2333/releases/download/v0.0.0/test.txt",
//         ));
//         assert!(!github_release_allow(
//             &config,
//             "sjtug/lug2/releases/download/v0.0.0/test.txt",
//         ));
//     }
// }
