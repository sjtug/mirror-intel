#[macro_use]
extern crate rocket;

use std::sync::Arc;

use prometheus::{Encoder, TextEncoder};
use reqwest::{Client, ClientBuilder};
use rocket::State;
use tokio::sync::mpsc::channel;
use tracing::{info, warn};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

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
fn setup_log() -> impl Drop {
    let registry = Registry::default().with(EnvFilter::from_default_env());
    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());

    let rust_log_format = std::env::var("RUST_LOG_FORMAT")
        .unwrap_or_default()
        .to_lowercase();

    let (json, after) =
        match (rust_log_format.as_str(), cfg!(debug_assertions)) {
            ("plain", _) => (false, None),
            ("json", _) => (true, None),
            ("", dev) => (!dev, None), // release defaults to json and debug to plain
            (format, dev) => (
                !dev,
                Some(move || {
                    warn!(
                "RUST_LOG_FORMAT is set to '{}', but mirror-intel is in {} mode. Using '{}'",
                format, if dev {"debug"} else {"release"}, if dev { "plain" } else { "json" }
            );
                }),
            ),
        };

    if json {
        tracing::subscriber::set_global_default(registry.with(JsonStorageLayer).with(
            BunyanFormattingLayer::new("mirror-intel".to_string(), writer),
        ))
        .expect("Unable to set logger");
    } else {
        tracing::subscriber::set_global_default(
            registry.with(fmt::Layer::default().pretty().with_writer(writer)),
        )
        .expect("Unable to set logger");
    };
    if let Some(after) = after {
        after();
    }
    guard
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
    LogTracer::init().unwrap();
    let _guard = setup_log();

    let rocket = rocket::ignite();
    let figment = rocket.figment();
    let config: Config = figment.extract().expect("config");

    info!("checking if bucket is available...");
    // check if credentials are set and we have permissions
    if !config.read_only {
        if let Err(error) = check_s3(&config.s3).await {
            warn!(
                ?error,
                "s3 storage backend not available, but not running in read-only mode"
            );
            // config.read_only = true;
        }
    }

    info!(?config, "config loaded");

    info!("starting server...");

    let metrics = Arc::new(Metrics::default());
    let metrics_download = metrics.clone();
    let tx = (!config.read_only).then(|| {
        // TODO so we are now having a global bounded queue, which will be easily blocked if there're
        // too many requests to large files. See issue #24.
        let (tx, rx) = channel(config.max_pending_task);

        let config_download = Arc::new(config.clone());

        // Spawn caching future.
        tokio::spawn(async move {
            download_artifacts(rx, Client::new(), config_download, metrics_download).await;
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
        s3_client: Arc::new(storage::get_anonymous_s3_client(&config.s3)),
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
