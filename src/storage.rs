//! S3 storage backend.
use std::time::Duration;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Region;
use tokio::time::timeout;

use crate::common::S3Config;
use crate::error::{Error, Result};

fn s3_region(s3_config: &S3Config) -> Region {
    Region::new(s3_config.name.clone())
}

/// Creates an authenticated S3 client.
///
/// The default credential provider is used.
fn get_s3_client(s3_config: &S3Config) -> S3Client {
    let s3_builder = aws_sdk_s3::Config::builder()
        .region(s3_region(s3_config))
        .endpoint_url(s3_config.endpoint.clone())
        .force_path_style(true);

    S3Client::from_conf(s3_builder.build())
}

/// Creates an anonymous S3 client.
///
/// It works in read-only mode.
pub fn get_anonymous_s3_client(s3_config: &S3Config) -> S3Client {
    S3Client::from_conf(
        aws_sdk_s3::Config::builder()
            .region(s3_region(s3_config))
            .endpoint_url(s3_config.endpoint.clone())
            .force_path_style(true)
            .allow_no_auth()
            .build(),
    )
}

/// Takes a stream and save it to S3 storage.
///
/// This function requires credentials to be present in default credential provider.
pub async fn stream_to_s3(
    path: &str,
    content_length: u64,
    stream: aws_sdk_s3::primitives::ByteStream,
    s3_config: &S3Config,
) -> Result<aws_sdk_s3::operation::put_object::PutObjectOutput> {
    let s3_client = get_s3_client(s3_config);

    Ok(s3_client
        .put_object()
        .body(stream)
        .bucket(s3_config.bucket.clone())
        .key(path)
        .content_length(content_length as i64)
        .send()
        .await?)
}

/// Check whether authenticated S3 storage is available.
pub async fn check_s3(s3_config: &S3Config) -> Result<()> {
    timeout(Duration::from_secs(1), async move {
        let s3_client = get_s3_client(s3_config);

        // s3_client
        //     .list_objects()
        //     .bucket(s3_config.bucket.clone())
        //     .send()
        //     .await?;
        if let Some(sentinel_object_key) = &s3_config.sentinel_object_key {
            s3_client
                .get_object()
                .bucket(s3_config.bucket.clone())
                .key(sentinel_object_key.clone())
                .range("bytes=0-0")
                .send()
                .await?;
        }
        // NOTE: this can be too heavy for jCloud S3, thus we check only sentinel object instead of listing all objects.
        else {
            s3_client
                .list_objects()
                .bucket(s3_config.bucket.clone())
                .send()
                .await?;
        }

        Ok::<(), Error>(())
    })
    .await
    .map_err(|err| Error::CustomError(format!("failed to check s3 storage {:?}", err)))?
}
