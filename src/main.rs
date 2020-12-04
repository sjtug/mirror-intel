#![feature(proc_macro_hygiene, decl_macro)]

mod artifacts;
mod common;
mod error;
mod repos;
mod storage;
mod utils;

use artifacts::download_artifacts;
use common::{IntelMission, MAX_PENDING_TASK};

use repos::{
    crates_io, fedora_iot, fedora_ostree, flathub, homebrew_bottles, pypi_packages, rust_static,
};
use storage::check_s3;

#[macro_use]
extern crate rocket;

use reqwest::Client;

use slog::{o, Drain};

use tokio::sync::mpsc::channel;

fn create_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();
    slog::Logger::root(drain, o!())
}

#[launch]
async fn rocket() -> rocket::Rocket {
    let logger = create_logger();

    info!("checking if bucket is available...");
    // check if credentials are set and we have permissions
    check_s3().await;
    info!("starting server...");

    let (tx, rx) = channel(MAX_PENDING_TASK);
    let client = Client::new();

    let mission = IntelMission { tx, client };

    tokio::spawn(async move { download_artifacts(rx, Client::new(), logger).await });

    rocket::ignite().manage(mission).mount(
        "/",
        routes![
            crates_io,
            flathub,
            fedora_ostree,
            fedora_iot,
            pypi_packages,
            homebrew_bottles,
            rust_static
        ],
    )
}
