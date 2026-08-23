use std::time::{Duration, Instant};

use moka::Expiry;
use moka::future::Cache;
use tracing::debug;

use crate::common::{Config, IntelMission, Task};

const PREFETCH_CACHE_MAX_CAPACITY: u64 = 10_000;
const DEFAULT_HEAD_PREFETCH_TTL_SECS: u64 = 60;
const HEAD_PREFETCH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreCacheStatus {
    // S3 object exists, or already known to exist
    S3,
    // Upstream object exists, or already known to exist
    Upstream,
    // Neither S3 nor upstream could confirm the object; caller returns 404
    None,
}

#[derive(Clone)]
pub struct PrefetchCache {
    cache: Cache<String, PreCacheStatus>,
}

impl PrefetchCache {
    pub fn new(ttl: Duration) -> Self {
        let ttl = non_zero_ttl(ttl);
        let negative_ttl = non_zero_ttl(ttl / 10);

        Self {
            cache: Cache::builder()
                .max_capacity(PREFETCH_CACHE_MAX_CAPACITY)
                .expire_after(PrefetchCacheExpiry { ttl, negative_ttl })
                .build(),
        }
    }

    pub async fn prefetch_cache_action(
        &self,
        task: &Task,
        mission: &IntelMission,
        config: &Config,
    ) -> PreCacheStatus {
        let key = task.root_path();

        self.cache
            .get_with(key, direct_prefetch_cache_action(task, mission, config))
            .await
    }
}

pub const fn default_head_prefetch_ttl() -> Duration {
    Duration::from_secs(DEFAULT_HEAD_PREFETCH_TTL_SECS)
}

struct PrefetchCacheExpiry {
    ttl: Duration,
    negative_ttl: Duration,
}

impl Expiry<String, PreCacheStatus> for PrefetchCacheExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &PreCacheStatus,
        _created_at: Instant,
    ) -> Option<Duration> {
        match value {
            PreCacheStatus::S3 | PreCacheStatus::Upstream => Some(self.ttl),
            PreCacheStatus::None => Some(self.negative_ttl),
        }
    }
}

fn non_zero_ttl(ttl: Duration) -> Duration {
    ttl.max(Duration::from_secs(1))
}

async fn direct_prefetch_cache_action(
    task: &Task,
    mission: &IntelMission,
    config: &Config,
) -> PreCacheStatus {
    // Probe S3 first and only fall back to upstream on an S3 miss. This runs
    // in the request-hot path for `RouteAction::Cache` (`repos.rs`), so the
    // sequential ordering is a deliberate performance tradeoff.
    if head_req(&mission.prefetch_client, task.cached_url(config)).await {
        return PreCacheStatus::S3;
    }

    // Mirror the download worker (`artifacts::run`) and rewrite the origin via
    // `endpoints.overrides` before probing, otherwise the HEAD targets the
    // configured origin that may be unreachable without the rewrite, causing
    // false-negative `None` results (and spurious 404s in the request path).
    let mut upstream_task = task.clone();
    upstream_task.apply_override(&config.endpoints.overrides);

    if head_req(&mission.prefetch_client, upstream_task.upstream_url()).await {
        PreCacheStatus::Upstream
    } else {
        PreCacheStatus::None
    }
}

async fn head_req(client: &reqwest::Client, url: url::Url) -> bool {
    match client
        .head(url.clone())
        .timeout(HEAD_PREFETCH_TIMEOUT)
        .send()
        .await
    {
        Ok(resp) => resp.status().is_success(),
        Err(error) => {
            debug!(?error, %url, "HEAD prefetch failed");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::join_all;
    use httpmock::{Method, MockServer};
    use reqwest::Client;
    use tokio::sync::mpsc::channel;

    use crate::common::{
        Config, EndpointOverride, IntelMission, Metrics, S3Config, S3Health, Task,
    };
    use crate::storage::get_anonymous_s3_client;

    use super::{PreCacheStatus, PrefetchCache};

    fn make_task(server: &MockServer) -> Task {
        Task {
            storage: "storage",
            origin: server.base_url(),
            path: "missing".to_string(),
            retry_limit: 3,
        }
    }

    fn make_config(server: &MockServer) -> Config {
        Config {
            s3: S3Config {
                name: "test".to_string(),
                region: "test".to_string(),
                endpoint: server.base_url(),
                website_endpoint: server.base_url(),
                bucket: "bucket".to_string(),
                healthcheck_key_prefix: ".health".to_string(),
                healthcheck_interval_secs: 300,
                healthcheck_timeout_secs: 30,
            },
            ..Default::default()
        }
    }

    fn make_mission(config: &Config, prefetch_cache: Arc<PrefetchCache>) -> IntelMission {
        let (tx, _rx) = channel(1024);

        IntelMission {
            tx: Some(tx),
            client: Client::new(),
            prefetch_client: Client::new(),
            metrics: Arc::new(Metrics::default()),
            s3_health: S3Health::healthy(),
            s3_client: Arc::new(get_anonymous_s3_client(&config.s3)),
            prefetch_cache,
        }
    }

    #[tokio::test]
    async fn must_cache_negative_prefetch_result() {
        let server = MockServer::start_async().await;
        let s3_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/bucket/storage/missing");
                then.status(404);
            })
            .await;
        let upstream_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/missing");
                then.status(404);
            })
            .await;
        let config = make_config(&server);
        let cache = Arc::new(PrefetchCache::new(Duration::from_secs(60)));
        let mission = make_mission(&config, cache.clone());
        let task = make_task(&server);

        assert_eq!(
            cache.prefetch_cache_action(&task, &mission, &config).await,
            PreCacheStatus::None
        );
        assert_eq!(
            cache.prefetch_cache_action(&task, &mission, &config).await,
            PreCacheStatus::None
        );

        s3_head.assert_calls_async(1).await;
        upstream_head.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn must_coalesce_concurrent_negative_prefetches() {
        let server = MockServer::start_async().await;
        let s3_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/bucket/storage/missing");
                then.status(404);
            })
            .await;
        let upstream_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/missing");
                then.status(404);
            })
            .await;
        let config = make_config(&server);
        let cache = Arc::new(PrefetchCache::new(Duration::from_secs(60)));
        let mission = make_mission(&config, cache.clone());
        let task = make_task(&server);

        let statuses =
            join_all((0..10).map(|_| cache.prefetch_cache_action(&task, &mission, &config))).await;

        assert!(
            statuses
                .iter()
                .all(|status| *status == PreCacheStatus::None)
        );
        s3_head.assert_calls_async(1).await;
        upstream_head.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn must_skip_upstream_probe_when_s3_hits() {
        let server = MockServer::start_async().await;
        let s3_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/bucket/storage/missing");
                then.status(200);
            })
            .await;
        let upstream_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/missing");
                then.status(200);
            })
            .await;
        let config = make_config(&server);
        let cache = Arc::new(PrefetchCache::new(Duration::from_secs(60)));
        let mission = make_mission(&config, cache.clone());
        let task = make_task(&server);

        assert_eq!(
            cache.prefetch_cache_action(&task, &mission, &config).await,
            PreCacheStatus::S3
        );

        s3_head.assert_calls_async(1).await;
        // S3 already had the object, so upstream must not be contacted.
        upstream_head.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn must_apply_endpoint_overrides_to_upstream_probe() {
        let server = MockServer::start_async().await;
        // S3 miss forces the upstream probe to run.
        let s3_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/bucket/storage/missing");
                then.status(404);
            })
            .await;
        // Upstream is reachable only because the override rewrites the origin
        // onto the mock server. Without the rewrite the probe would target an
        // unroutable host and report `None`.
        let upstream_head = server
            .mock_async(|when, then| {
                when.method(Method::HEAD).path("/missing");
                then.status(200);
            })
            .await;

        let mut config = make_config(&server);
        config.endpoints.overrides = vec![EndpointOverride {
            name: "rewrite-blocked".to_string(),
            pattern: "https://blocked.invalid".to_string(),
            replace: server.base_url(),
        }];
        let cache = Arc::new(PrefetchCache::new(Duration::from_secs(60)));
        let mission = make_mission(&config, cache.clone());
        let task = Task {
            storage: "storage",
            origin: "https://blocked.invalid".to_string(),
            path: "missing".to_string(),
            retry_limit: 3,
        };

        assert_eq!(
            cache.prefetch_cache_action(&task, &mission, &config).await,
            PreCacheStatus::Upstream
        );

        s3_head.assert_calls_async(1).await;
        upstream_head.assert_calls_async(1).await;
    }
}
