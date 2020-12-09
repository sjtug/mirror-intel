use crate::common::{Config, IntelMission};
use crate::error::{Error, Result};
use crate::utils::{decode_path, resolve_object, resolve_ostree};

use std::path::PathBuf;

use rocket::State;
use rocket::{
    http::ContentType,
    response::{Content, Redirect},
};

#[get("/crates.io/<path..>")]
pub async fn crates_io(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    resolve_object(
        "crates.io",
        decode_path(&path)?,
        &config.endpoints.crates_io,
        &intel_mission,
    )
    .await
}

#[get("/flathub/<path..>")]
pub async fn flathub(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = &config.endpoints.flathub;
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("flathub", path, origin, &intel_mission).await
}

#[get("/fedora-ostree/<path..>")]
pub async fn fedora_ostree(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = &config.endpoints.fedora_ostree;
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("fedora-ostree", path, origin, &intel_mission).await
}

#[get("/fedora-iot/<path..>")]
pub async fn fedora_iot(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = &config.endpoints.fedora_iot;
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("fedora-iot", path, origin, &intel_mission).await
}

#[get("/pypi-packages/<path..>")]
pub async fn pypi_packages(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    let origin = &config.endpoints.pypi_packages;
    if let Some(name) = path.file_name() {
        if let Some(name) = name.to_str() {
            if name.contains("-py2") && !name.contains("py3") {
                // ignore python2 only packages
                let path = decode_path(&path)?;
                return Ok(Redirect::moved(format!("{}/{}", origin, path)));
            }
        }
    }

    resolve_object("pypi-packages", decode_path(&path)?, origin, &intel_mission).await
}

#[get("/homebrew-bottles/<path..>")]
pub async fn homebrew_bottles(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    resolve_object(
        "homebrew-bottles",
        decode_path(&path)?,
        &config.endpoints.homebrew_bottles,
        &intel_mission,
    )
    .await
}

#[get("/rust-static/<path..>")]
pub async fn rust_static(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    let origin = &config.endpoints.rustup;

    if let Some(name) = path.file_name() {
        if let Some(name) = name.to_str() {
            if name.starts_with("channel-") || name.ends_with(".toml") {
                let path = decode_path(&path)?;
                // mirrors.tuna will rewrite channel toml, and would make rustup to redirect to TUNA.
                return Ok(Redirect::moved(format!(
                    "https://static.rust-lang.org/{}",
                    path
                )));
            }
        }
    }

    let path = decode_path(&path)?;

    if !path.starts_with("dist") && !path.starts_with("rustup") {
        return Ok(Redirect::moved(format!("{}/{}", origin, path)));
    }
    resolve_object("rust-static", path, origin, &intel_mission).await
}

#[derive(Debug, Responder)]
pub enum DartResponse {
    Content(Content<String>),
    Redirect(Redirect),
}

impl From<Redirect> for DartResponse {
    fn from(res: Redirect) -> Self {
        Self::Redirect(res)
    }
}

impl From<Content<String>> for DartResponse {
    fn from(res: Content<String>) -> Self {
        Self::Content(res)
    }
}

#[get("/dart-pub/<path..>")]
pub async fn dart_pub(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<DartResponse> {
    let origin = &config.endpoints.dart_pub;
    let path = decode_path(&path)?;
    if path.starts_with("api/") {
        let upstream = format!("{}/{}", origin, path);
        let response = intel_mission.client.get(&upstream).send().await?;
        if let Some(content_length) = response.content_length() {
            if content_length > 4 * 1024 * 1024 {
                // redirect to upstream if the object is too big
                return Ok(Redirect::moved(upstream).into());
            }
        }
        if !response.status().is_success() {
            return Ok(Redirect::found(upstream).into());
        }
        // otherwise, rewrite content
        let response = response.text().await?;
        let response = response.replace(origin, &format!("{}/dart-pub", config.base_url));
        Ok(Content(ContentType::JSON, response).into())
    } else {
        Ok(
            resolve_object("dart-pub", path, origin, &intel_mission)
                .await?
                .into(),
        )
    }
}

#[get("/guix/<path..>")]
pub async fn guix(
    path: PathBuf,
    intel_mission: State<'_, IntelMission>,
    config: State<'_, Config>,
) -> Result<Redirect> {
    let origin = &config.endpoints.guix;
    let path = decode_path(&path)?;
    if path.starts_with("nar/") || path.ends_with(".narinfo") {
        Ok(resolve_object("guix", path, origin, &intel_mission).await?)
    } else {
        let upstream = format!("{}/{}", origin, path);
        return Ok(Redirect::moved(upstream).into());
    }
}
