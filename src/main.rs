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
use slog::{o, Drain};
use slog_global::{info, warn};
use std::sync::Arc;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct Task {
    pub storage: String,
    pub origin: String,
    pub path: String,
}

const MAX_PENDING_TASK: usize = 1024 * 16;
const MAX_CONCURRENT_DOWNLOAD: usize = 512;

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
    resolve_object("crates.io", decode_path(&path)?, "https://static.crates.io").await
}

fn resolve_ostree(origin: &str, path: &str) -> Option<Redirect> {
    if path.starts_with("summary")
        || path.starts_with("config")
        || path.starts_with("summaries")
        || path.starts_with("refs/")
    {
        return Some(Redirect::permanent(format!("{}/{}", origin, path)));
    }
    None
}

fn decode_path(path: &PathBuf) -> Result<&str> {
    Ok(path.to_str().ok_or_else(|| Error::DecodePathError(()))?)
}

#[get("/flathub/<path..>")]
async fn flathub(path: PathBuf) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = "https://dl.flathub.org/repo";
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("flathub", path, origin).await
}

#[get("/fedora-ostree/<path..>")]
async fn fedora_ostree(path: PathBuf) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = "https://d2uk5hbyrobdzx.cloudfront.net";
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("fedora-ostree", path, origin).await
}

#[get("/fedora-iot/<path..>")]
async fn fedora_iot(path: PathBuf) -> Result<Redirect> {
    let path = decode_path(&path)?;
    let origin = "https://d2ju0wfl996cmc.cloudfront.net";
    if let Some(redir) = resolve_ostree(origin, path) {
        return Ok(redir);
    }
    resolve_object("fedora-iot", path, origin).await
}

#[get("/pypi-packages/<path..>")]
async fn pypi_packages(path: PathBuf) -> Result<Redirect> {
    resolve_object(
        "pypi-packages",
        decode_path(&path)?,
        // "https://files.pythonhosted.org/packages",
        "https://mirrors.tuna.tsinghua.edu.cn/pypi/web/packages",
    )
    .await
}

#[get("/homebrew-bottles/<path..>")]
async fn homebrew_bottles(path: PathBuf) -> Result<Redirect> {
    resolve_object(
        "homebrew-bottles",
        decode_path(&path)?,
        "https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles/bottles",
    )
    .await
}

#[get("/npm-registry/<path..>")]
async fn npm_registry(path: PathBuf) -> Result<Redirect> {
    resolve_object(
        "homebrew-bottles",
        decode_path(&path)?,
        "https://registry.npmjs.org",
    )
    .await
}

#[get("/rust-static/<path..>")]
async fn rust_static(path: PathBuf) -> Result<Redirect> {
    let origin = "https://mirrors.sjtug.sjtu.edu.cn/rust-static";

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
    resolve_object("rust-static", path, origin).await
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
) -> Result<(u64, impl futures::Stream<Item = reqwest::Result<Bytes>>)> {
    let response = client.get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    Ok((response.content_length().unwrap(), response.bytes_stream()))
}

fn get_s3_client() -> S3Client {
    S3Client::new(rusoto_core::Region::Custom {
        name: "jCloud S3".to_string(),
        endpoint: "https://s3.jcloud.sjtu.edu.cn".to_string(),
    })
}

pub async fn stream_to_s3(
    path: &str,
    content_length: u64,
    stream: rusoto_s3::StreamingBody,
) -> Result<rusoto_s3::PutObjectOutput> {
    let s3_client = get_s3_client();

    let mut req = rusoto_s3::PutObjectRequest::default();
    req.body = Some(stream);
    req.bucket = S3_BUCKET.to_string();
    req.key = path.to_string();
    req.content_length = Some(content_length as i64);
    Ok(s3_client.put_object(req).await?)
}

pub fn transform_stream(
    stream: impl futures::Stream<Item = reqwest::Result<Bytes>>,
) -> impl futures::Stream<Item = std::result::Result<Bytes, std::io::Error>> {
    stream.map(|x| {
        x.map_err(|err| {
            warn!("failed to receive data: {:?}", err);
            std::io::Error::new(std::io::ErrorKind::Other, err)
        })
    })
}

async fn process_task(task: Task) -> Result<()> {
    let (content_length, stream) = stream_from_url(CLIENT.clone(), &task.origin).await?;
    info!("get {}, length={}", task.path, content_length);
    let stream = transform_stream(stream);
    let key = format!("{}/{}", task.storage, task.path);
    stream_to_s3(&key, content_length, rusoto_s3::StreamingBody::new(stream)).await?;
    info!("upload {} {} to bucket", task.storage, task.path);
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
            if let Err(err) = process_task(task).await {
                warn!("{:?}", err);
            }
        });
    }
}

async fn check_s3() {
    let s3_client = get_s3_client();
    let mut req = rusoto_s3::ListObjectsRequest::default();
    req.bucket = S3_BUCKET.to_string();
    s3_client.list_objects(req).await.unwrap();
}

#[launch]
async fn rocket() -> rocket::Rocket {
    let _guard = slog_global::set_global(create_logger());

    info!("checking if bucket is available...");
    // check if credentials are set and we have permissions
    check_s3().await;
    info!("starting server...");

    tokio::spawn(async move { download_artifacts().await });

    rocket::ignite().mount(
        "/",
        routes![
            crates_io,
            flathub,
            fedora_ostree,
            fedora_iot,
            pypi_packages,
            homebrew_bottles,
            npm_registry,
            rust_static
        ],
    )
}
