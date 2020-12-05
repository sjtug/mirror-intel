#![feature(proc_macro_hygiene, decl_macro)]

mod artifacts;
mod common;
mod error;
mod repos;
mod storage;
mod utils;

use artifacts::download_artifacts;
use common::{Config, IntelMission, Metrics};
use error::{Error, Result};
use repos::{
    crates_io, fedora_iot, fedora_ostree, flathub, homebrew_bottles, pypi_packages, rust_static,
};
use storage::check_s3;

#[macro_use]
extern crate rocket;

use std::sync::Arc;

use prometheus::{Encoder, TextEncoder};
use reqwest::{header, Client};
use rocket::State;
use slog::{o, Drain};
use tokio::sync::mpsc::channel;

fn create_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();
    slog::Logger::root(drain, o!())
}

#[get("/metrics")]
pub async fn metrics(intel_mission: State<'_, IntelMission>) -> Result<Vec<u8>> {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = intel_mission.metrics.registry.gather();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|err| Error::CustomError(format!("failed to encode metrics: {:?}", err)))?;
    Ok(buffer)
}

#[launch]
async fn rocket() -> rocket::Rocket {
    let logger = create_logger();

    info!("checking if bucket is available...");
    // check if credentials are set and we have permissions
    check_s3().await;
    info!("starting server...");

    let rocket = rocket::ignite();
    let figment = rocket.figment();
    let config: Config = figment.extract().expect("config");

    info!("{:?}", config);

    let (tx, rx) = channel(config.max_pending_task);
    let client = Client::new();

    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::USER_AGENT,
        header::HeaderValue::from_str(&config.user_agent).unwrap(),
    );

    let mission = IntelMission {
        tx: tx.clone(),
        client,
        metrics: Arc::new(Metrics::new()),
    };

    let config_download = config.clone();
    let metrics_download = mission.metrics.clone();
    tokio::spawn(async move {
        download_artifacts(
            rx,
            tx,
            Client::new(),
            logger,
            &config_download,
            metrics_download,
        )
        .await
    });

    rocket.manage(mission).manage(config).mount(
        "/",
        routes![
            crates_io,
            flathub,
            fedora_ostree,
            fedora_iot,
            pypi_packages,
            homebrew_bottles,
            rust_static,
            metrics
        ],
    )
}
