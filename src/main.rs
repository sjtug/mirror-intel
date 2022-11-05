#![allow(
    clippy::future_not_send,
    clippy::cast_possible_truncation,
    clippy::module_name_repetitions,
    clippy::enum_variant_names,
    clippy::case_sensitive_file_extension_comparisons,
    clippy::cast_possible_wrap,
    clippy::missing_errors_doc
)]

use std::sync::Arc;

use actix_web::{guard, web, App, HttpServer};
use prometheus::{Encoder, TextEncoder};
use reqwest::{Client, ClientBuilder};
use tokio::sync::mpsc::channel;
use tracing::{info, warn};
use tracing_actix_web::TracingLogger;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

use artifacts::download_artifacts;
use browse::list;
use common::{Config, IntelMission, Metrics};
use error::{Error, Result};
use queue::queue_length;
use repos::{configure_repo_routes, index};
use storage::check_s3;
use utils::not_found;

mod artifacts;
mod browse;
mod common;
mod error;
mod intel_path;
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
#[allow(clippy::unused_async)]
pub async fn metrics_endpoint(intel_mission: web::Data<IntelMission>) -> Result<Vec<u8>> {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = intel_mission.metrics.gather();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|err| Error::CustomError(format!("failed to encode metrics: {:?}", err)))?;
    Ok(buffer)
}

#[tokio::main]
async fn main() {
    LogTracer::init().unwrap();
    let _guard = setup_log();

    let config: Arc<Config> = Arc::new(common::collect_config());

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

        let config_download = config.clone();

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

    let addr = config.address.clone();
    let port = config.port;
    let workers = config.workers;

    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(mission.clone()))
            .app_data(web::Data::from(config.clone()))
            .route("/metrics", web::get().to(metrics_endpoint))
            .route(
                "/{path:.*}",
                web::get()
                    .guard(guard::fn_guard(|ctx| {
                        ctx.head().uri.query() == Some("mirror_intel_list")
                    }))
                    .to(list),
            )
            .configure(configure_repo_routes)
            .route("/{path:.*}", web::get().to(index))
            .default_service(web::route().to(not_found))
            .wrap_fn(queue_length)
            .wrap(TracingLogger::default())
    });

    if let Some(workers) = workers {
        server = server.workers(workers);
    }

    server.bind((&*addr, port)).unwrap().run().await.unwrap();
}
