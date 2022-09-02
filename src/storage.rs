//! S3 storage backend.
use std::time::Duration;

use rusoto_core::credential::{AwsCredentials, StaticProvider};
use rusoto_core::Region;
use rusoto_s3::{S3Client, S3};
use tokio::time::timeout;

use crate::common::S3Config;
use crate::error::{Error, Result};

impl From<&S3Config> for Region {
    fn from(s3_config: &S3Config) -> Self {
        Self::Custom {
            name: s3_config.name.clone(),
            endpoint: s3_config.endpoint.clone(),
        }
    }
}

/// Creates an authenticated S3 client.
///
/// The default credential provider is used.
fn get_s3_client(s3_config: &S3Config) -> S3Client {
    S3Client::new(s3_config.into())
}

/// Creates an anonymous S3 client.
///
/// It works in read-only mode.
pub fn get_anonymous_s3_client(s3_config: &S3Config) -> S3Client {
    S3Client::new_with(
        rusoto_core::request::HttpClient::new().expect("Failed to creat HTTP client"),
        StaticProvider::from(AwsCredentials::default()),
        s3_config.into(),
    )
}

/// Takes a stream and save it to S3 storage.
///
/// This function requires credentials to be present in default credential provider.
pub async fn stream_to_s3(
    path: &str,
    content_length: u64,
    stream: rusoto_s3::StreamingBody,
    s3_config: &S3Config,
) -> Result<rusoto_s3::PutObjectOutput> {
    let s3_client = get_s3_client(s3_config);

    let req = rusoto_s3::PutObjectRequest {
        body: Some(stream),
        bucket: s3_config.bucket.clone(),
        key: path.to_string(),
        content_length: Some(content_length as i64),
        ..Default::default()
    };
    Ok(s3_client.put_object(req).await?)
}

/// Check whether authenticated S3 storage is available.
pub async fn check_s3(s3_config: &S3Config) -> Result<()> {
    timeout(Duration::from_secs(1), async move {
        let s3_client = get_s3_client(s3_config);
        let req = rusoto_s3::ListObjectsRequest {
            bucket: s3_config.bucket.clone(),
            ..Default::default()
        };
        s3_client.list_objects(req).await?;
        Ok::<(), Error>(())
    })
    .await
    .map_err(|err| Error::CustomError(format!("failed to check s3 storage {:?}", err)))?
}
