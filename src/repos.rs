use crate::error::Result;
use crate::intel_path::IntelPath;
use crate::intel_query::IntelQuery;
use crate::{
    common::{Config, IntelMission, IntelResponse, Task},
    utils,
};

use lazy_static::lazy_static;
use paste::paste;
use regex::Regex;
use rocket::response::Redirect;
use rocket::State;

macro_rules! simple_intel {
    ($name:ident, $route:expr, $filter:ident, $proxy:ident) => {
        paste! {
            #[route(GET, path = "/" $route "/<path..>?<query..>")]
            pub async fn [<$name _get>](
                path: IntelPath,
                query: IntelQuery,
                intel_mission: State<'_, IntelMission>,
                config: State<'_, Config>,
            ) -> Result<IntelResponse<'static>> {
                let origin = config.endpoints.$name.clone();
                let path = path.into();
                let task = Task {
                    storage: $route,
                    ttl: config.ttl,
                    origin,
                    path,
                };

                if !query.is_empty() {
                    return Ok(Redirect::found(format!("{}?{}", task.upstream(), query.to_string())).into());
                }

                if !$filter(&config, &task.path) {
                    return Ok(Redirect::moved(task.upstream()).into());
                }

                if $proxy(&task.path) {
                    return Ok(task
                        .resolve_upstream()
                        .reverse_proxy(&intel_mission)
                        .await?
                        .into());
                }

                Ok(task
                    .resolve(&intel_mission, &config)
                    .await?
                    .stream_small_cached(config.direct_stream_size_kb, &intel_mission, &config)
                    .await?
                    .into())
            }

            #[route(HEAD, path = "/" $route "/<path..>?<query..>")]
            pub async fn [<$name _head>](
                path: IntelPath,
                query: IntelQuery,
                intel_mission: State<'_, IntelMission>,
                config: State<'_, Config>,
            ) -> Result<IntelResponse<'static>> {
                let origin = config.endpoints.$name.clone();
                let path = path.into();
                let task = Task {
                    storage: $route,
                    ttl: config.ttl,
                    origin,
                    path,
                };

                if !query.is_empty() {
                    return Ok(Redirect::found(format!("{}?{}", task.upstream(), query.to_string())).into());
                }

                if !$filter(&config, &task.path) {
                    return Ok(Redirect::moved(task.upstream()).into());
                }

                if $proxy(&task.path) {
                    return Ok(rocket::Response::build()
                        .status(rocket::http::Status::Ok)
                        .finalize()
                        .into());
                }

                Ok(task
                    .resolve_no_content(&intel_mission, &config).await?
                    .redirect(&config)
                    .into())
            }
        }
    };
}

pub fn disallow_all(_path: &str) -> bool {
    false
}

pub fn allow_all(_config: &Config, _path: &str) -> bool {
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

simple_intel! { crates_io, "crates.io", allow_all, disallow_all }
simple_intel! { flathub, "flathub", ostree_allow, disallow_all }
simple_intel! { fedora_ostree, "fedora-ostree", ostree_allow, disallow_all }
simple_intel! { fedora_iot, "fedora-iot", ostree_allow, disallow_all }
simple_intel! { pypi_packages, "pypi-packages", allow_all, disallow_all }
simple_intel! { homebrew_bottles, "homebrew-bottles", allow_all, disallow_all }
simple_intel! { linuxbrew_bottles, "linuxbrew-bottles", linuxbrew_allow, disallow_all }
simple_intel! { rust_static, "rust-static", rust_static_allow, disallow_all }
simple_intel! { pytorch_wheels, "pytorch-wheels", wheels_allow, wheels_proxy }
simple_intel! { sjtug_internal, "sjtug-internal", sjtug_internal_allow, disallow_all }
simple_intel! { flutter_infra, "flutter_infra", flutter_allow, disallow_all }
simple_intel! { flutter_infra_release, "flutter_infra_release", flutter_allow, disallow_all }
simple_intel! { github_release, "github-release", github_release_allow, disallow_all }
simple_intel! { opam_cache, "opam-cache", allow_all, disallow_all }
simple_intel! { gradle_distribution, "gradle/distributions", gradle_allow, disallow_all }

#[get("/dart-pub/<path..>?<query..>")]
pub async fn dart_pub(
    path: IntelPath,
    query: IntelQuery,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.dart_pub.clone();
    let path = path.into();
    let task = Task {
        storage: "dart-pub",
        ttl: config.ttl,
        origin: origin.clone(),
        path,
    };

    if !query.is_empty() {
        return Ok(Redirect::found(format!("{}?{}", task.upstream(), query.to_string())).into());
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
            .await?
            .into())
    } else if task.path.starts_with("packages/") {
        Ok(task
            .resolve(&intel_mission, &config)
            .await?
            .stream_small_cached(config.direct_stream_size_kb, &intel_mission, &config)
            .await?
            .into())
    } else {
        Ok(Redirect::moved(task.upstream()).into())
    }
}

#[get("/pypi/web/simple/<path..>?<query..>")]
pub async fn pypi(
    path: IntelPath,
    query: IntelQuery,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.pypi_simple.clone();
    let path = path.into();
    let task = Task {
        storage: "pypi",
        ttl: config.ttl,
        origin: origin.clone(),
        path,
    };

    if !query.is_empty() {
        return Ok(Redirect::found(format!("{}?{}", task.upstream(), query.to_string())).into());
    }

    Ok(task
        .resolve_upstream()
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
        .await?
        .into())
}

macro_rules! nix_intel {
    ($name:ident, $route:expr) => {
        paste! {
            #[route(GET, path = "/" $route "/<path..>?<query..>")]
            pub async fn [<$name>](
                path: IntelPath,
                query: IntelQuery,
                intel_mission: State<'_, IntelMission>,
                config: State<'_, Config>,
            ) -> Result<IntelResponse<'static>> {
                let origin = config.endpoints.$name.clone();
                let path = path.into();
                let task = Task {
                    storage: $route,
                    ttl: config.ttl,
                    origin,
                    path,
                };

                if !query.is_empty() {
                    return Ok(Redirect::found(format!("{}?{}", task.upstream(), query.to_string())).into());
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
                    Ok(Redirect::moved(task.upstream()).into())
                }
            }
        }
    };
}

nix_intel! { guix, "guix" }
nix_intel! { nix_channels_store, "nix-channels/store" }

#[get("/<path..>")]
pub async fn index(path: IntelPath, config: State<'_, Config>) -> IntelResponse<'static> {
    let path: String = path.into();
    if config
        .endpoints
        .s3_only
        .iter()
        .any(|x| path.starts_with(x) && &path != x)
    {
        return Redirect::moved(format!(
            "{}/{}/{}",
            config.s3.endpoint, config.s3.bucket, path
        ))
        .into();
    }
    utils::no_route_for(&path).into()
}

#[cfg(test)]
mod tests {
    use crate::queue::QueueLength;
    use crate::utils::not_found;
    use crate::{
        common::{Config, EndpointOverride, IntelMission, Metrics},
        storage::get_anonymous_s3_client,
    };
    use reqwest::ClientBuilder;
    use rocket::http::Status;
    use std::sync::Arc;
    use tokio::sync::mpsc::{channel, Receiver};

    use super::*;

    async fn make_rocket() -> (rocket::local::asynchronous::Client, Config, Receiver<Task>) {
        use rocket::local::asynchronous::Client;
        let rocket = rocket::ignite();
        let figment = rocket.figment();
        let mut config: Config = figment.extract().expect("config");
        config.read_only = true;

        let (tx, rx) = channel(1024);
        let client = ClientBuilder::new()
            .user_agent(&config.user_agent)
            .build()
            .unwrap();

        let mission = IntelMission {
            tx: Some(tx),
            client,
            metrics: Arc::new(Metrics::new()),
            s3_client: Arc::new(get_anonymous_s3_client()),
        };

        let queue_length_fairing = QueueLength {
            mission: mission.clone(),
        };

        let rocket = rocket
            .manage(mission)
            .manage(config.clone())
            .attach(queue_length_fairing)
            .register(catchers![not_found])
            .mount(
                "/",
                routes![
                    sjtug_internal_head,
                    sjtug_internal_get,
                    pytorch_wheels_head,
                    pytorch_wheels_get
                ],
            );

        (
            Client::tracked(rocket)
                .await
                .expect("valid rocket instance"),
            config,
            rx,
        )
    }

    fn exist_object() -> Task {
        Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz".to_string(),
            ttl: 3,
        }
    }

    fn missing_object() -> Task {
        Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/mirror-clone-2333.tar.gz".to_string(),
            ttl: 3,
        }
    }

    fn forbidden_object() -> Task {
        Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/forbidden/mirror-clone.tar.gz".to_string(),
            ttl: 3,
        }
    }

    #[rocket::async_test]
    async fn test_redirect_exist_get() {
        // if an object exists in s3, we should permanently redirect users to s3
        let (client, config, _rx) = make_rocket().await;
        let object = exist_object();
        let response = client.get(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::MovedPermanently);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.cached(&config)]
        );
    }

    #[rocket::async_test]
    async fn test_redirect_exist_head() {
        let (client, config, _rx) = make_rocket().await;
        let object = exist_object();
        let response = client.head(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::MovedPermanently);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.cached(&config)]
        );
    }

    #[rocket::async_test]
    async fn test_redirect_missing_get() {
        // if an object doesn't exist in s3, we should temporarily redirect users to upstream
        let (client, _, _rx) = make_rocket().await;
        let object = missing_object();
        let response = client.get(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::Found);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.upstream()]
        );
    }

    #[rocket::async_test]
    async fn test_redirect_missing_head() {
        // if an object doesn't exist in s3, we should temporarily redirect users to upstream
        let (client, _, _rx) = make_rocket().await;
        let object = missing_object();
        let response = client.head(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::Found);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.upstream()]
        );
    }

    #[rocket::async_test]
    async fn test_redirect_forbidden_get() {
        // if an object is filtered, we should permanently redirect users to upstream
        let (client, _, _rx) = make_rocket().await;
        let object = forbidden_object();
        let response = client.get(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::MovedPermanently);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.upstream()]
        );
    }

    #[rocket::async_test]
    async fn test_redirect_forbidden_head() {
        // if an object is filtered, we should permanently redirect users to upstream
        let (client, _, _rx) = make_rocket().await;
        let object = forbidden_object();
        let response = client.head(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::MovedPermanently);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.upstream()]
        );
    }

    #[rocket::async_test]
    async fn test_url_segment() {
        // this case is to test if we could process escaped URL correctly
        let (client, _, _rx) = make_rocket().await;
        let object = Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/mirror%2B%2B%2B-clone.tar.gz".to_string(),
            ttl: 3,
        };
        let response = client.head(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::Found);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.upstream()]
        );
    }

    #[rocket::async_test]
    async fn test_url_segment_fail() {
        // this case is to test if we could process escaped URL correctly
        let (client, _, _rx) = make_rocket().await;
        let object = Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path: "mirror-clone/releases/download/v0.1.7/.mirror%2B%2B%2B-clone.tar.gz".to_string(),
            ttl: 3,
        };
        let response = client.head(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::NotFound);
    }

    #[rocket::async_test]
    async fn test_url_segment_query() {
        // this case is to test if we could process escaped URL correctly
        let (client, _, _rx) = make_rocket().await;
        let object = Task {
            storage: "sjtug-internal",
            origin: "https://github.com/sjtug".to_string(),
            path:
                "mirror-clone/releases/download/v0.1.7/mirror-clone.tar.gz?ci=233333&ci2=23333333"
                    .to_string(),
            ttl: 3,
        };
        let response = client.get(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::Found);
        assert_eq!(
            response.headers().get("Location").collect::<Vec<&str>>(),
            vec![&object.upstream()]
        );
    }

    #[test]
    fn test_flutter_allow() {
        let config = Config::default();
        assert!(!flutter_allow(&config, "releases/releases_windows.json"));
        assert!(!flutter_allow(&config, "releases/releases_linux.json"));
        assert!(flutter_allow(
            &config,
            "releases/stable/linux/flutter_linux_1.17.0-stable.tar.xz"
        ));
        assert!(flutter_allow(
            &config,
            "flutter/069b3cf8f093d44ec4bae1319cbfdc4f8b4753b6/android-arm/artifacts.zip"
        ));
        assert!(flutter_allow(
            &config,
            "flutter/fonts/03bdd42a57aff5c496859f38d29825843d7fe68e/fonts.zip"
        ));
        assert!(!flutter_allow(&config, "flutter/coverage/lcov.info"));
    }

    #[test]
    fn test_task_override() {
        let mut task = Task {
            storage: "flutter_infra",
            ttl: 233,
            origin: "https://storage.flutter-io.cn/".to_string(),
            path: "test".to_string(),
        };
        task.to_download_task(&[EndpointOverride {
            name: "flutter".to_string(),
            pattern: "https://storage.flutter-io.cn/".to_string(),
            replace: "https://storage.googleapis.com/".to_string(),
        }]);
        assert_eq!(task.origin, "https://storage.googleapis.com/");
    }

    #[rocket::async_test]
    async fn test_proxy_head() {
        // if an object doesn't exist in s3, we should temporarily redirect users to upstream
        let (client, _, _rx) = make_rocket().await;
        let object = Task {
            storage: "pytorch-wheels",
            origin: "https://download.pytorch.org/whl".to_string(),
            path: "torch_stable.html".to_string(),
            ttl: 3,
        };
        let response = client.head(object.root_path()).dispatch().await;
        assert_eq!(response.status(), Status::Ok);
    }

    #[rocket::async_test]
    async fn test_github_release() {
        let mut config = Config::default();
        config.github_release.allow.push("sjtug/lug/".to_string());
        assert!(github_release_allow(
            &config,
            "sjtug/lug/releases/download/v0.0.0/test.txt"
        ));
        assert!(!github_release_allow(
            &config,
            "sjtug/lug/2333/releases/download/v0.0.0/test.txt"
        ));
        assert!(!github_release_allow(
            &config,
            "sjtug/lug2/releases/download/v0.0.0/test.txt"
        ));
    }
}
