use crate::error::{Error, Result};
use crate::common::S3_BUCKET;

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



pub async fn check_s3() {
    let s3_client = get_s3_client();
    let mut req = rusoto_s3::ListObjectsRequest::default();
    req.bucket = S3_BUCKET.to_string();
    s3_client.list_objects(req).await.unwrap();
}