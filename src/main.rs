#[macro_use]
extern crate rocket;

use std::sync::Arc;

use prometheus::{Encoder, TextEncoder};
use reqwest::{Client, ClientBuilder};
use rocket::State;
use slog::{info, warn};
use slog::{o, Drain};
use tokio::sync::mpsc::channel;

use artifacts::download_artifacts;
use browse::list;
use common::{Config, IntelMission, Metrics};
use error::{Error, Result};
use queue::QueueLength;
use repos::*;
use storage::check_s3;
use utils::not_found;

mod artifacts;
mod browse;
mod common;
mod error;
mod intel_path;
mod intel_query;
mod queue;
mod repos;
mod storage;
mod utils;

/// Create a logger with styled output, env-filter, and async logging.
/// TODO what about a global logger? There's no reason to pass it around.
fn create_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();
    slog::Logger::root(drain, o!())
}

/// Metrics endpoint.
#[get("/metrics")]
pub async fn metrics(intel_mission: State<'_, IntelMission>) -> Result<Vec<u8>> {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = intel_mission.metrics.gather();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|err| Error::CustomError(format!("failed to encode metrics: {:?}", err)))?;
    Ok(buffer)
}

#[launch]
async fn rocket() -> rocket::Rocket {
    let logger = create_logger();
    let rocket = rocket::ignite();
    let figment = rocket.figment();
    let config: Config = figment.extract().expect("config");

    info!(logger, "checking if bucket is available...");
    // check if credentials are set and we have permissions
    if let Err(error) = check_s3(&config.s3.bucket).await {
        warn!(logger, "s3 storage backend not available, but not running in read-only mode"; "error" => format!("{:?}", error));
        // config.read_only = true;
    }

    info!(logger, "{:?}", config);

    info!(logger, "starting server...");

    let metrics = Arc::new(Metrics::default());
    let metrics_download = metrics.clone();
    let tx = (!config.read_only).then(|| {
        // TODO so we are now having a global bounded queue, which will be easily blocked if there're
        // too many requests to large files. See issue #24.
        let (tx, rx) = channel(config.max_pending_task);

        let config_download = Arc::new(config.clone());

        // Spawn caching future.
        tokio::spawn(async move {
            download_artifacts(rx, Client::new(), logger, config_download, metrics_download).await;
        });

        tx
    });

    let client = ClientBuilder::new()
        .user_agent(&config.user_agent)
        .build()
        .unwrap();

    let mission = IntelMission {
        tx,
        client,
        metrics,
        s3_client: Arc::new(storage::get_anonymous_s3_client()),
    };

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
                list,
                crates_io_get,
                crates_io_head,
                flathub_get,
                flathub_head,
                fedora_ostree_get,
                fedora_ostree_head,
                fedora_iot_get,
                fedora_iot_head,
                pypi_packages_get,
                pypi_packages_head,
                homebrew_bottles_get,
                homebrew_bottles_head,
                rust_static_get,
                rust_static_head,
                dart_pub,
                guix,
                pytorch_wheels_get,
                pytorch_wheels_head,
                linuxbrew_bottles_get,
                linuxbrew_bottles_head,
                sjtug_internal_get,
                sjtug_internal_head,
                flutter_infra_get,
                flutter_infra_head,
                github_release_get,
                github_release_head,
                nix_channels_store,
                pypi,
                opam_cache_head,
                opam_cache_get,
                gradle_distribution_head,
                gradle_distribution_get,
                metrics,
                index
            ],
        )
}
