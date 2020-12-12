use crate::common::{Config, IntelMission, IntelResponse, Task};
use crate::error::Result;
use crate::utils::{decode_path, ostree_ignore};

use std::path::PathBuf;

use lazy_static::lazy_static;
use regex::Regex;
use rocket::response::Redirect;
use rocket::State;

#[get("/crates.io/<path..>")]
pub async fn crates_io(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.crates_io.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "crates.io",
        ttl: config.ttl,
        origin,
        path,
    };

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

#[get("/flathub/<path..>")]
pub async fn flathub(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.flathub.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "flathub",
        ttl: config.ttl,
        origin,
        path,
    };

    if ostree_ignore(&task.path) {
        return Ok(Redirect::moved(task.upstream()).into());
    }

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

#[get("/fedora-ostree/<path..>")]
pub async fn fedora_ostree(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.fedora_ostree.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "fedora-ostree",
        ttl: config.ttl,
        origin,
        path,
    };

    if ostree_ignore(&task.path) {
        return Ok(Redirect::moved(task.upstream()).into());
    }

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

#[get("/fedora-iot/<path..>")]
pub async fn fedora_iot(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.fedora_iot.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "fedora-iot",
        ttl: config.ttl,
        origin,
        path,
    };

    if ostree_ignore(&task.path) {
        return Ok(Redirect::moved(task.upstream()).into());
    }

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

#[get("/pypi-packages/<path..>")]
pub async fn pypi_packages(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.pypi_packages.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "pypi-packages",
        ttl: config.ttl,
        origin,
        path,
    };

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

#[get("/homebrew-bottles/<path..>")]
pub async fn homebrew_bottles(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.homebrew_bottles.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "homebrew-bottles",
        ttl: config.ttl,
        origin,
        path,
    };

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

#[get("/linuxbrew-bottles/<path..>")]
pub async fn linuxbrew_bottles(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.linuxbrew_bottles.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "linuxbrew-bottles",
        ttl: config.ttl,
        origin,
        path,
    };

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

#[get("/rust-static/<path..>")]
pub async fn rust_static(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.rustup.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "rust-static",
        ttl: config.ttl,
        origin,
        path,
    };

    if task.path.contains("channel-") || task.path.ends_with(".toml") {
        return Ok(Redirect::moved(task.upstream()).into());
    }

    if !task.path.starts_with("dist") && !task.path.starts_with("rustup") {
        return Ok(Redirect::moved(task.upstream()).into());
    }

    Ok(task
        .resolve(&intel_mission)
        .await?
        .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
        .await?
        .into())
}

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

#[get("/pytorch-wheels/<path..>")]
pub async fn pytorch_wheels(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = config.endpoints.pytorch_wheels.clone();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "pytorch-wheels",
        ttl: config.ttl,
        origin,
        path,
    };

    if task.path.ends_with(".whl") {
        Ok(task
            .resolve(&intel_mission)
            .await?
            .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
            .await?
            .into())
    } else {
        Ok(Redirect::moved(task.upstream()).into())
    }
}

lazy_static! {}

#[get("/sjtug-internal/<path..>")]
pub async fn sjtug_internal(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<IntelResponse<'static>> {
    let origin = "https://github.com/sjtug".to_string();
    let path = decode_path(&path)?.to_string();
    let task = Task {
        storage: "sjtug-internal",
        ttl: config.ttl,
        origin,
        path,
    };

    lazy_static! {
        static ref REGEX: Regex =
            Regex::new("^([^/]*)/releases/download/v([^/]*)/[^/]*.tar.gz$").unwrap();
    };

    if REGEX.is_match(&task.path) {
        Ok(task
            .resolve(&intel_mission)
            .await?
            .stream_small_cached(config.direct_stream_size_kb, &intel_mission)
            .await?
            .into())
    } else {
        Ok(Redirect::moved(task.upstream()).into())
    }
}
