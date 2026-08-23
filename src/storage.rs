//! S3 storage backend.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Region, RequestChecksumCalculation};
use aws_sdk_s3::primitives::ByteStream;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

use crate::common::{Metrics, S3Config, S3Health, S3HealthStatus};
use crate::error::{Error, Result};

const HEALTHCHECK_PAYLOAD: &[u8] = b"mirror-intel PutObject health check\n";
static HEALTHCHECK_ID: AtomicU64 = AtomicU64::new(0);

fn s3_region(s3_config: &S3Config) -> Region {
    Region::new(if s3_config.region.is_empty() {
        "default".to_string()
    } else {
        s3_config.region.clone()
    })
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Custom(format!("{} is missing or empty", name)))
}

/// Creates an authenticated S3 client from the standard AWS environment variables.
pub fn get_s3_client(s3_config: &S3Config) -> Result<S3Client> {
    let credentials = Credentials::new(
        required_env("AWS_ACCESS_KEY_ID")?,
        required_env("AWS_SECRET_ACCESS_KEY")?,
        std::env::var("AWS_SESSION_TOKEN")
            .ok()
            .filter(|value| !value.is_empty()),
        None,
        "environment",
    );
    let s3_builder = aws_sdk_s3::Config::builder()
        .region(s3_region(s3_config))
        .endpoint_url(s3_config.endpoint.clone())
        .force_path_style(true)
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired);

    Ok(S3Client::from_conf(s3_builder.build()))
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

/// Takes a stream and saves it to S3 storage with a reusable authenticated client.
pub async fn stream_to_s3(
    client: &S3Client,
    path: &str,
    content_length: u64,
    stream: aws_sdk_s3::primitives::ByteStream,
    s3_config: &S3Config,
) -> Result<aws_sdk_s3::operation::put_object::PutObjectOutput> {
    Ok(client
        .put_object()
        .body(stream)
        .bucket(s3_config.bucket.clone())
        .key(path)
        .content_length(content_length as i64)
        .send()
        .await?)
}

fn healthcheck_key(s3_config: &S3Config) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = HEALTHCHECK_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}/{}-{}-{}",
        s3_config.healthcheck_key_prefix.trim_end_matches('/'),
        std::process::id(),
        timestamp,
        id
    )
}

async fn delete_healthcheck_object(
    client: &S3Client,
    s3_config: &S3Config,
    key: &str,
    operation_timeout: Duration,
) -> Result<()> {
    let result = timeout(
        operation_timeout,
        client
            .delete_object()
            .bucket(s3_config.bucket.clone())
            .key(key)
            .send(),
    )
    .await
    .map_err(|_| Error::Timeout)
    .and_then(|result| result.map_err(Error::from));

    if let Err(error) = &result {
        warn!(
            ?error,
            key, "s3 PutObject health-check cleanup failed; canary may remain"
        );
    }
    result.map(|_| ())
}

/// Test authenticated PutObject access and remove the temporary object afterwards.
pub async fn check_s3_put_object(client: &S3Client, s3_config: &S3Config) -> Result<()> {
    let key = healthcheck_key(s3_config);
    let operation_timeout = Duration::from_secs(s3_config.healthcheck_timeout_secs.max(1));
    let put_result = timeout(
        operation_timeout,
        client
            .put_object()
            .body(ByteStream::from_static(HEALTHCHECK_PAYLOAD))
            .bucket(s3_config.bucket.clone())
            .key(&key)
            .content_length(HEALTHCHECK_PAYLOAD.len() as i64)
            .send(),
    )
    .await;

    match put_result {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            // The server may have committed the object before returning an error.
            let _ = delete_healthcheck_object(client, s3_config, &key, operation_timeout).await;
            return Err(error.into());
        }
        Err(_) => {
            // A timed-out request has an unknown outcome, so always attempt cleanup.
            let _ = delete_healthcheck_object(client, s3_config, &key, operation_timeout).await;
            return Err(Error::Timeout);
        }
    }

    delete_healthcheck_object(client, s3_config, &key, operation_timeout).await
}

pub fn mark_s3_degraded(health: &S3Health, metrics: &Metrics, error: &Error) {
    metrics.s3_put_object_healthy.set(0);
    if health.set(S3HealthStatus::Degraded) != S3HealthStatus::Degraded {
        warn!(
            ?error,
            status = "degraded",
            "s3 upload health status changed"
        );
    }
}

fn record_healthcheck_result(health: &S3Health, metrics: &Metrics, result: &Result<()>) {
    match result {
        Ok(()) => {
            metrics.s3_put_object_healthy.set(1);
            if health.set(S3HealthStatus::Healthy) != S3HealthStatus::Healthy {
                info!(status = "healthy", "s3 upload health status changed");
            } else {
                debug!("s3 PutObject health check succeeded");
            }
        }
        Err(error) => {
            let previous = health.status();
            mark_s3_degraded(health, metrics, error);
            if previous == S3HealthStatus::Degraded {
                warn!(?error, "s3 PutObject health check remains degraded");
            }
        }
    }
}

/// Run one PutObject health check immediately.
pub async fn initialize_s3_health(
    client: &S3Client,
    s3_config: &S3Config,
    health: &S3Health,
    metrics: &Metrics,
) {
    let result = check_s3_put_object(client, s3_config).await;
    record_healthcheck_result(health, metrics, &result);
}

/// Periodically refresh authenticated S3 upload health.
pub async fn monitor_s3_health(
    client: S3Client,
    s3_config: S3Config,
    health: S3Health,
    metrics: Arc<Metrics>,
) {
    let interval = Duration::from_secs(s3_config.healthcheck_interval_secs.max(1));
    loop {
        sleep(interval).await;
        let result = check_s3_put_object(&client, &s3_config).await;
        record_healthcheck_result(&health, &metrics, &result);
    }
}

#[cfg(test)]
mod tests {
    use httpmock::Method::{DELETE, PUT};
    use httpmock::MockServer;
    use serial_test::serial;

    use super::*;

    fn test_config(server: &MockServer) -> S3Config {
        S3Config {
            name: "test".into(),
            region: "test".into(),
            endpoint: server.base_url(),
            website_endpoint: server.base_url(),
            bucket: "bucket".into(),
            healthcheck_key_prefix: ".health".into(),
            healthcheck_interval_secs: 300,
            healthcheck_timeout_secs: 30,
        }
    }

    fn set_test_credentials() {
        // Environment mutation is process-global; these tests are serialized.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test-access-key");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test-secret-key");
        }
    }

    #[tokio::test]
    #[serial]
    async fn put_healthcheck_deletes_canary() {
        set_test_credentials();
        let server = MockServer::start_async().await;
        let put = server.mock(|when, then| {
            when.method(PUT).path_matches(r"^/bucket/\.health/.*");
            then.status(200);
        });
        let delete = server.mock(|when, then| {
            when.method(DELETE).path_matches(r"^/bucket/\.health/.*");
            then.status(204);
        });

        let config = test_config(&server);
        let client = get_s3_client(&config).unwrap();
        check_s3_put_object(&client, &config).await.unwrap();

        put.assert_calls(1);
        delete.assert_calls(1);
    }

    #[test]
    fn healthcheck_result_updates_status_and_metric() {
        let health = S3Health::default();
        let metrics = Metrics::default();
        let failure = Err(Error::Custom("test failure".into()));

        assert_eq!(metrics.s3_put_object_healthy.get(), -1);
        record_healthcheck_result(&health, &metrics, &failure);
        assert_eq!(health.status(), S3HealthStatus::Degraded);
        assert_eq!(metrics.s3_put_object_healthy.get(), 0);

        record_healthcheck_result(&health, &metrics, &Ok(()));
        assert_eq!(health.status(), S3HealthStatus::Healthy);
        assert_eq!(metrics.s3_put_object_healthy.get(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn delete_failure_degrades_healthcheck() {
        set_test_credentials();
        let server = MockServer::start_async().await;
        let put = server.mock(|when, then| {
            when.method(PUT).path_matches(r"^/bucket/\.health/.*");
            then.status(200);
        });
        let delete = server.mock(|when, then| {
            when.method(DELETE).path_matches(r"^/bucket/\.health/.*");
            then.status(500);
        });
        let config = test_config(&server);
        let client = get_s3_client(&config).unwrap();

        let error = check_s3_put_object(&client, &config).await.unwrap_err();

        assert!(matches!(error, Error::DeleteObject(_)));
        put.assert_calls(1);
        assert!(delete.calls() >= 1, "cleanup must be attempted");
    }
}
