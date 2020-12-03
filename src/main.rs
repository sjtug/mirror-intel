#![feature(proc_macro_hygiene, decl_macro)]

mod error;
use error::{Error, Result};

use std::path::PathBuf;

#[macro_use]
extern crate rocket;
use futures_util::StreamExt;
use lazy_static::lazy_static;
use reqwest::{Client, StatusCode};
use rocket::http::hyper::Bytes;
use rocket::response::Redirect;
use rusoto_s3::{S3Client, S3};
use slog::{o, Drain, Level, LevelFilter};
use slog_scope::{info, warn};
use slog_scope_futures::FutureExt;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct Task {
    pub storage: String,
    pub origin: String,
    pub path: String,
}

const MAX_PENDING_TASK: usize = 1024;
const MAX_CONCURRENT_DOWNLOAD: usize = 16;

const S3_BUCKET: &str = "899a892efef34b1b944a19981040f55b-oss01";

lazy_static! {
    static ref CLIENT: Client = Client::new();
    static ref TASK_CHAN: (Sender<Task>, Arc<Mutex<Receiver<Task>>>) = {
        let (tx, rx) = channel(MAX_PENDING_TASK);
        (tx, Arc::new(Mutex::new(rx)))
    };
}

#[get("/crates.io/<path..>")]
async fn crates_io(path: PathBuf) -> Result<Redirect> {
    resolve_object(
        "crates.io_test",
        &path.to_str().ok_or_else(|| Error::DecodePathError(()))?,
        "https://static.crates.io",
    )
    .await
}

async fn resolve_object(storage: &str, path: &str, origin: &str) -> Result<Redirect> {
    let s3 = format!(
        "https://s3.jcloud.sjtu.edu.cn/{}/{}/{}",
        S3_BUCKET, storage, path
    );
    let origin = format!("{}/{}", origin, path);
    if let Ok(resp) = CLIENT.head(&s3).send().await {
        match resp.status() {
            StatusCode::OK => return Ok(Redirect::temporary(s3)),
            StatusCode::FORBIDDEN => {
                TASK_CHAN
                    .0
                    .clone()
                    .send(Task {
                        storage: storage.to_string(),
                        path: path.to_string(),
                        origin: origin.to_string(),
                    })
                    .await
                    .map_err(|_| Error::SendError(()))?;
            }
            _ => {}
        }
    }
    Ok(Redirect::temporary(origin))
}

fn create_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();
    slog::Logger::root(drain, o!())
}

pub async fn stream_from_url(
    client: Client,
    url: &str,
) -> Result<impl futures::Stream<Item = reqwest::Result<Bytes>>> {
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    Ok(response.bytes_stream())
}

pub async fn stream_to_s3(path: &str, stream: rusoto_s3::StreamingBody) -> Result<rusoto_s3::PutObjectOutput> {
    let s3_client = S3Client::new(rusoto_core::Region::UsEast1);
    let mut req = rusoto_s3::PutObjectRequest::default();
    req.body = Some(stream);
    req.bucket = S3_BUCKET.to_string();
    req.key = path.to_string();
    Ok(s3_client.put_object(req).await?)
}

pub fn transform_stream(
    stream: impl futures::Stream<Item = reqwest::Result<Bytes>>,
) -> impl futures::Stream<Item = std::result::Result<Bytes, std::io::Error>> {
    stream.map(|x| {
        x.map_err(|err| {
            println!("failed to receive data: {:?}", err);
            std::io::Error::new(std::io::ErrorKind::Other, err)
        })
    })
}

async fn process_task(task: Task) -> Result<()> {
    let stream = stream_from_url(CLIENT.clone(), &task.origin).await?;
    let stream = transform_stream(stream);
    let key = format!("{}/{}", task.storage, task.path);
    let output = stream_to_s3(&key, rusoto_s3::StreamingBody::new(stream)).await?;
    println!("upload {} {} to bucket, {:?}", task.storage, task.path, output);
    Ok(())
}

async fn download_artifacts() {
    let mut rx = TASK_CHAN.1.lock().await;
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOAD));
    while let Some(task) = rx.recv().await {
        info!("task received {:?}", task);
        let permit = Arc::clone(&sem).acquire_owned().await;
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = process_task(task)
                .await
            {
                println!("{:?}", err);
            }
        });
    }
}

#[launch]
fn rocket() -> rocket::Rocket {
    let _guard = slog_scope::set_global_logger(create_logger());

    tokio::spawn(async move {
        download_artifacts()
            .with_logger(&slog_scope::logger())
            .await
    });

    rocket::ignite().mount("/", routes![crates_io])
}
