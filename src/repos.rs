use crate::common::IntelMission;
use crate::error::{Error, Result};
use crate::utils::{decode_path, resolve_object, resolve_ostree};

use std::path::PathBuf;

use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use rocket::http::hyper::Bytes;
use rocket::response::Redirect;
use rocket::State;
use rusoto_s3::{S3Client, S3};
use slog::{o, Drain};
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Semaphore;

#[get("/crates.io/<path..>")]
pub async fn crates_io(path: PathBuf, intel_mission: State<'_, IntelMission>) -> Result<Redirect> {
    resolve_object(
        "crates.io",
        decode_path(&path)?,
        "https://static.crates.io",
        &intel_mission,
    )
    .await
}

#[get("/flathub/<path..>")]
pub async fn flathub(path: PathBuf, intel_mission: State<'_, IntelMission>) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = "https://dl.flathub.org/repo";
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("flathub", path, origin, &intel_mission).await
}

#[get("/fedora-ostree/<path..>")]
pub async fn fedora_ostree(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = "https://d2uk5hbyrobdzx.cloudfront.net";
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("fedora-ostree", path, origin, &intel_mission).await
}

#[get("/fedora-iot/<path..>")]
pub async fn fedora_iot(path: PathBuf, intel_mission: State<'_, IntelMission>) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = "https://d2ju0wfl996cmc.cloudfront.net";
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("fedora-iot", path, origin, &intel_mission).await
}

#[get("/pypi-packages/<path..>")]
pub async fn pypi_packages(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
) -> Result<Redirect> {
    resolve_object(
        "pypi-packages",
        decode_path(&path)?,
        // "https://files.pythonhosted.org/packages",
        "https://mirrors.tuna.tsinghua.edu.cn/pypi/web/packages",
        &intel_mission,
    )
    .await
}

#[get("/homebrew-bottles/<path..>")]
pub async fn homebrew_bottles(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
) -> Result<Redirect> {
    resolve_object(
        "homebrew-bottles",
        decode_path(&path)?,
        "https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles",
        &intel_mission,
    )
    .await
}

#[get("/rust-static/<path..>")]
pub async fn rust_static(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
) -> Result<Redirect> {
    let origin = "https://mirrors.tuna.tsinghua.edu.cn/rustup";

    if let Some(name) = path.file_name() {
        if let Some(name) = name.to_str() {
            if name.starts_with("channel-") || name.ends_with(".toml") {
                let path = decode_path(&path)?;
                return Ok(Redirect::permanent(format!("{}/{}", origin, path)));
            }
        }
    }

    let path = decode_path(&path)?;

    if !path.starts_with("dist") && !path.starts_with("rustup") {
        return Ok(Redirect::permanent(format!("{}/{}", origin, path)));
    }
    resolve_object("rust-static", path, origin, &intel_mission).await
}
