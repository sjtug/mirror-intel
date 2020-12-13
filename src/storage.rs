use crate::common::S3_BUCKET;
use crate::error::{Error, Result};

use rusoto_s3::{S3Client, S3};
use std::time::Duration;
use tokio::time::timeout;

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

pub async fn check_s3() -> Result<()> {
    Ok(timeout(Duration::from_secs(3), async move {
        let s3_client = get_s3_client();
        let mut req = rusoto_s3::ListObjectsRequest::default();
        req.bucket = S3_BUCKET.to_string();
        s3_client.list_objects(req).await?;
        Ok::<(), Error>(())
    })
    .await
    .map_err(|err| Error::CustomError(format!("failed to check s3 storage {:?}", err)))??)
}
