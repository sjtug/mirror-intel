use crate::common::{Config, IntelMission, IntelResponse, Task};
use crate::error::Result;
use crate::utils::decode_path;

use std::path::PathBuf;

use lazy_static::lazy_static;
use paste::paste;
use regex::Regex;
use rocket::response::Redirect;
use rocket::State;

macro_rules! simple_intel {
    ($name:ident, $route:expr, $filter:ident) => {
        paste! {
            #[route(GET, path = "/" $route "/<path..>")]
            pub async fn [<$name _get>](
                path: PathBuf,
                intel_mission: State<'_, IntelMission>,
                config: State<'_, Config>,
            ) -> Result<IntelResponse<'static>> {
                let origin = config.endpoints.$name.clone();
                let path = decode_path(&path)?.to_string();
                let task = Task {
                    storage: $route,
                    ttl: config.ttl,
                    origin,
                    path,
                };

                if !$filter(&task.path) {
                    return Ok(Redirect::moved(task.upstream()).into());
                }

                Ok(task
                    .resolve(&intel_mission, &config)
                    .await?
                    .stream_small_cached(config.direct_stream_size_kb, &intel_mission, &config)
                    .await?
                    .into())
            }

            #[route(HEAD, path = "/" $route "/<path..>")]
            pub async fn [<$name _head>](
                path: PathBuf,
                intel_mission: State<'_, IntelMission>,
                config: State<'_, Config>,
            ) -> Result<Redirect> {
                let origin = config.endpoints.$name.clone();
                let path = decode_path(&path)?.to_string();
                let task = Task {
                    storage: $route,
                    ttl: config.ttl,
                    origin,
                    path,
                };


                if !$filter(&task.path) {
                    return Ok(Redirect::moved(task.upstream()));
                }

                Ok(task
                    .resolve_no_content(&intel_mission, &config).await?
                    .redirect(&config))
            }
        }
    };
}

pub fn allow_all(_path: &str) -> bool {
    true
}

pub fn ostree_allow(path: &str) -> bool {
    !(path.starts_with("summary") || path.starts_with("config") || path.starts_with("refs/"))
}

pub fn rust_static_allow(path: &str) -> bool {
    if path.contains("channel-") || path.ends_with(".toml") {
        return false;
    }

    if !path.starts_with("dist") && !path.starts_with("rustup") {
        return false;
    }

    true
}

pub fn wheels_allow(path: &str) -> bool {
    path.ends_with(".whl")
}

pub fn github_releases_allow(path: &str) -> bool {
    lazy_static! {
        static ref REGEX: Regex =
            Regex::new("^[^/]*/releases/download/[^/]*/[^/]*.(tar.gz|zip)$").unwrap();
    };

    REGEX.is_match(path)
}

simple_intel! { crates_io, "crates.io", allow_all }
simple_intel! { flathub, "flathub", ostree_allow }
simple_intel! { fedora_ostree, "fedora-ostree", ostree_allow }
simple_intel! { fedora_iot, "fedora-iot", ostree_allow }
simple_intel! { pypi_packages, "pypi-packages", allow_all }
simple_intel! { homebrew_bottles, "homebrew-bottles", allow_all }
simple_intel! { linuxbrew_bottles, "linuxbrew-bottles", allow_all }
simple_intel! { rust_static, "rust-static", rust_static_allow }
simple_intel! { pytorch_wheels, "pytorch-wheels", wheels_allow }
simple_intel! { sjtug_internal, "sjtug-internal", github_releases_allow }

#[get("/dart-pub/<path..>")]
pub async fn dart_pub(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.dart_pub.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "dart-pub",
        ttl: config.ttl,
        origin: origin.clone(),
        path,
    };

    if task.path == "api/packages" {
        Ok(Redirect::moved(task.upstream()).into())
    } else if task.path.starts_with("api/") {
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
    } else {
        Ok(task
            .resolve(&intel_mission, &config)
            .await?
            .stream_small_cached(config.direct_stream_size_kb, &intel_mission, &config)
            .await?
            .into())
    }
}

#[get("/guix/<path..>")]
pub async fn guix(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.guix.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "guix-test",
        ttl: config.ttl,
        origin,
        path,
    };

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

#[cfg(test)]
mod tests {
    use crate::common::{Config, IntelMission, Metrics};
    use crate::queue::QueueLength;
    use crate::utils::not_found;
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
            tx,
            client,
            metrics: Arc::new(Metrics::new()),
        };

        let queue_length_fairing = QueueLength {
            mission: mission.clone(),
        };

        let rocket = rocket
            .manage(mission)
            .manage(config.clone())
            .attach(queue_length_fairing)
            .register(catchers![not_found])
            .mount("/", routes![sjtug_internal_get, sjtug_internal_head]);

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
}
