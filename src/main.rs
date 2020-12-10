#![feature(proc_macro_hygiene, decl_macro)]

mod artifacts;
mod common;
mod error;
mod queue;
mod repos;
mod storage;
mod utils;

use artifacts::download_artifacts;
use common::{Config, IntelMission, Metrics};
use error::{Error, Result};
use queue::QueueLength;
use repos::{
    crates_io, dart_pub, fedora_iot, fedora_ostree, flathub, guix, homebrew_bottles, pypi_packages,
    rust_static,
};
use slog::info;
use storage::check_s3;

#[macro_use]
extern crate rocket;

use std::sync::Arc;

use prometheus::{Encoder, TextEncoder};
use reqwest::{Client, ClientBuilder};
use rocket::{Request, State};
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

#[catch(404)]
fn not_found(req: &Request) -> String {
    format!("No route for {}. mirror-intel uses S3-like storage backend, which means that you could not browse files like other mirror sites. Please follow our instructions to set up your software registry.", req.uri())
}

#[launch]
async fn rocket() -> rocket::Rocket {
    let logger = create_logger();

    info!(logger, "checking if bucket is available...");
    // check if credentials are set and we have permissions
    check_s3().await;
    info!(logger, "starting server...");

    let rocket = rocket::ignite();
    let figment = rocket.figment();
    let config: Config = figment.extract().expect("config");

    info!(logger, "{:?}", config);

    let (tx, rx) = channel(config.max_pending_task);
    let client = ClientBuilder::new()
        .user_agent(&config.user_agent)
        .build()
        .unwrap();

    let mission = IntelMission {
        tx,
        client,
        metrics: Arc::new(Metrics::new()),
    };

    let config_download = config.clone();
    let metrics_download = mission.metrics.clone();

    tokio::spawn(async move {
        download_artifacts(
            rx,
            Client::new(),
            logger,
            &config_download,
            metrics_download,
        )
        .await
    });

    let queue_length_fairing = QueueLength {
        mission: mission.clone(),
    };

    rocket
        .manage(mission)
        .manage(config)
        .attach(queue_length_fairing)
        .register(catchers![not_found])
        .mount(
            "/",
            routes![
                crates_io,
                flathub,
                fedora_ostree,
                fedora_iot,
                pypi_packages,
                homebrew_bottles,
                rust_static,
                dart_pub,
                guix,
                metrics
            ],
        )
}
