//! S3 storage backend.
use crate::error::{Error, Result};

use rusoto_core::credential::{AwsCredentials, StaticProvider};
use rusoto_core::Region;
use rusoto_s3::{S3Client, S3};
use std::time::Duration;
use tokio::time::timeout;

/// SJTU JCloud S3 region.
fn jcloud_region() -> Region {
    Region::Custom {
        name: "jCloud S3".to_string(),
        endpoint: "https://s3.jcloud.sjtu.edu.cn".to_string(),
    }
}

/// Creates an authenticated S3 client.
///
/// The default credential provider is used.
fn get_s3_client() -> S3Client {
    S3Client::new(jcloud_region())
}

/// Creates an anonymous S3 client.
///
/// It works in read-only mode.
pub fn get_anonymous_s3_client() -> S3Client {
    S3Client::new_with(
        rusoto_core::request::HttpClient::new().expect("Failed to creat HTTP client"),
        StaticProvider::from(AwsCredentials::default()),
        jcloud_region(),
    )
}

/// Takes a stream and save it to S3 storage.
///
/// This function requires credentials to be present in default credential provider.
pub async fn stream_to_s3(
    path: &str,
    content_length: u64,
    stream: rusoto_s3::StreamingBody,
    s3_bucket: &str,
) -> Result<rusoto_s3::PutObjectOutput> {
    let s3_client = get_s3_client();

    let mut req = rusoto_s3::PutObjectRequest::default();
    req.body = Some(stream);
    req.bucket = s3_bucket.to_string();
    req.key = path.to_string();
    req.content_length = Some(content_length as i64);
    Ok(s3_client.put_object(req).await?)
}

/// Check whether authenticated S3 storage is available.
pub async fn check_s3(bucket: &str) -> Result<()> {
    Ok(timeout(Duration::from_secs(1), async move {
        let s3_client = get_s3_client();
        let mut req = rusoto_s3::ListObjectsRequest::default();
        req.bucket = bucket.to_string();
        s3_client.list_objects(req).await?;
        Ok::<(), Error>(())
    })
    .await
    .map_err(|err| Error::CustomError(format!("failed to check s3 storage {:?}", err)))??)
}
