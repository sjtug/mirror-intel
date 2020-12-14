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
                let origin = config.endpoints.crates_io.clone();
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
                    .resolve(&intel_mission)
                    .await?
                    .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
                    .await?
                    .into())
            }

            #[route(HEAD, path = "/" $route "/<path..>")]
            pub async fn [<$name _head>](
                path: PathBuf,
                intel_mission: State<'_, IntelMission>,
                config: State<'_, Config>,
            ) -> Result<()> {
                let origin = config.endpoints.crates_io.clone();
                let path = decode_path(&path)?.to_string();
                let task = Task {
                    storage: $route,
                    ttl: config.ttl,
                    origin,
                    path,
                };

                if !$filter(&task.path) {
                    return Ok(());
                }

                Ok(task
                    .resolve_no_content(&intel_mission)
                    .await?)
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
            Regex::new("^[^/]*/releases/download/[^/]*/[^/]*.tar.gz$").unwrap();
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
            .rewrite_upstream(&intel_mission, 4096, |content| {
                content.replace(&origin, &format!("{}/dart-pub", config.base_url))
            })
            .await?
            .into())
    } else {
        Ok(task
            .resolve(&intel_mission)
            .await?
            .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
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
            .resolve(&intel_mission)
            .await?
            .reverse_proxy(&intel_mission)
            .await?
            .into())
    } else if task.path.ends_with(".narinfo") {
        Ok(task
            .resolve(&intel_mission)
            .await?
            .reverse_proxy(&intel_mission)
            .await?
            .into())
    } else {
        Ok(Redirect::moved(task.upstream()).into())
    }
}
