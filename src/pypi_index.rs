use reqwest::Client;
use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED};
use scraper::{Html, Selector};
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{RwLock, atomic::AtomicBool},
};
use tokio::time::MissedTickBehavior;
use tracing::{debug, warn};
use url::Url;

use crate::common::{Config, PypiIndexConfig};
use crate::error::Error;

/// Cached data for a single index page
#[derive(Debug, Clone, Default)]
pub struct CachedPage {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub links: Vec<String>,
}

/// Cache for PyPI index pages with ETag/Last-Modified support
#[derive(Debug, Default)]
pub struct PypiIndexCache {
    pages: HashMap<String, CachedPage>,
}

impl PypiIndexCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get cached page data for a relative path
    pub fn get(&self, relative_path: &str) -> Option<&CachedPage> {
        self.pages.get(relative_path)
    }

    /// Insert or update cached page data
    pub fn insert(&mut self, relative_path: String, page: CachedPage) {
        self.pages.insert(relative_path, page);
    }

    /// Get all valid index paths (non-empty paths with links)
    pub fn valid_pages(&self) -> Vec<String> {
        let mut pages: Vec<_> = self
            .pages
            .iter()
            .filter(|(path, cached)| !path.is_empty() && !cached.links.is_empty())
            .map(|(path, _)| path.clone())
            .collect();
        pages.sort();
        pages
    }
}

/// Result of a conditional fetch operation
#[derive(Debug)]
pub enum FetchResult {
    /// Page was fetched successfully (200 OK)
    Fetched {
        html: String,
        etag: Option<String>,
        last_modified: Option<String>,
    },
    /// Page was not modified (304 Not Modified)
    NotModified,
    /// Fetch failed
    Error(Error),
}

/// Parse the HTML content of a PyPI index page and extract all href attributes (links) from anchor tags.
pub fn parse_pypi_index(html: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("a").unwrap();
    document
        .select(&selector)
        .filter_map(|element| element.value().attr("href"))
        .map(|href| href.to_string())
        .collect()
}

/// Normalize a URL path to remove trailing separators (except root "/").
fn normalize_absolute_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Normalize a relative path to avoid leading/trailing separators.
fn normalize_relative_path(path: &str) -> String {
    path.trim_start_matches('/')
        .trim_end_matches('/')
        .to_string()
}

/// Return the directory form of an absolute path, ensuring it ends with "/".
fn directory_path(path: &str) -> String {
    let normalized = normalize_absolute_path(path);
    if normalized == "/" {
        normalized
    } else {
        format!("{normalized}/")
    }
}

/// Return a copy of the URL with path normalized as a directory.
fn as_directory_url(url: &Url) -> Url {
    let mut directory = url.clone();
    directory.set_path(&directory_path(url.path()));
    directory
}

/// Given a root URL and a target URL, return the path of the target URL relative to the root URL
/// if they share the same scheme, host, and port.
fn path_relative_to_root(root: &Url, target: &Url) -> Option<String> {
    // Ensure the target URL is within the same origin as the root URL.
    if root.scheme() != target.scheme()
        || root.host_str() != target.host_str()
        || root.port_or_known_default() != target.port_or_known_default()
    {
        return None;
    }

    let root_path = normalize_absolute_path(root.path());
    let target_path = normalize_absolute_path(target.path());
    if target_path == root_path {
        return Some(String::new());
    }

    let prefix = directory_path(&root_path);
    target_path
        .strip_prefix(&prefix)
        .map(normalize_relative_path)
}

/// Check if the link points to a large file.
fn is_largefile_link(link: &str) -> bool {
    let trimmed = link
        .split_once('#')
        .map_or(link, |(before_fragment, _)| before_fragment)
        .split_once('?')
        .map_or(link, |(before_query, _)| before_query);
    trimmed.ends_with(".whl")
        || trimmed.ends_with(".tar.gz")
        || trimmed.ends_with(".tar.bz2")
        || trimmed.ends_with(".tar.xz")
        || trimmed.ends_with(".tar.zst")
        || trimmed.ends_with(".zip")
        || trimmed.ends_with(".exe")
}

/// Given a parent page URL and a link (href) found in the page, resolve the link to an absolute URL,
///
/// # Arguments
/// * `root`: The root URL of the PyPI index, used to ensure the resolved URL is within the same domain.
/// * `parent`: The parent page URL
/// * `link`: The link (href) found in the parent page
///
/// # Return
/// * `Option<(String, Url)>`:
///   The relative path to the root URL, and the resolved absolute URL.
///   Returns None if the link cannot be resolved or is outside the root URL.
///
/// # Example
/// ```
/// let root = Url::parse("https://download.pytorch.org/whl").unwrap();
/// let parent = Url::parse("https://download.pytorch.org/whl/torch/").unwrap();
/// let link = "../cu130/torch/";
/// let Some((relative, child)) = resolve_child_url(&root, &parent, link) else {
///     panic!("Failed to resolve child URL");
/// };
/// assert_eq!(relative, "cu130/torch");
/// assert_eq!(child.as_str(), "https://download.pytorch.org/whl/cu130/torch/");
/// ```
///
fn resolve_child_url(root: &Url, parent: &Url, link: &str) -> Option<(String, Url)> {
    let child = as_directory_url(parent).join(link).ok()?;
    let relative = path_relative_to_root(root, &child)?;
    if relative.is_empty() {
        return None;
    }

    Some((relative, child))
}

/// Fetch a page with conditional GET support (ETag/Last-Modified).
/// Returns NotModified if the page hasn't changed, or Fetched with new content.
async fn fetch_page_conditional(
    client: &Client,
    url: &Url,
    cached: Option<&CachedPage>,
) -> FetchResult {
    let mut request = client.get(url.clone());

    // Add conditional headers if we have cached data
    if let Some(cached) = cached {
        if let Some(ref etag) = cached.etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        if let Some(ref last_modified) = cached.last_modified {
            request = request.header(IF_MODIFIED_SINCE, last_modified);
        }
    }

    let response = match request.send().await {
        Ok(resp) => resp,
        Err(e) => return FetchResult::Error(e.into()),
    };

    // 304 Not Modified - use cached data
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return FetchResult::NotModified;
    }

    // Check for errors
    if !response.status().is_success() {
        return FetchResult::Error(Error::HTTPError(response.status()));
    }

    // Extract caching headers
    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let last_modified = response
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Get body
    match response.text().await {
        Ok(html) => FetchResult::Fetched {
            html,
            etag,
            last_modified,
        },
        Err(e) => FetchResult::Error(e.into()),
    }
}

/// Fetch PyPI index with caching support, depth limits, and page limits.
///
/// On first call (empty cache), performs full BFS crawl up to max_depth.
/// On subsequent calls, uses conditional requests to only re-fetch changed pages.
///
/// # Arguments
/// * `client`: HTTP client for requests
/// * `url`: Root PyPI index URL
/// * `cache`: Mutable cache to store/retrieve page data
/// * `config`: PyPI index configuration (max_depth, max_pages)
///
/// # Returns
/// * `Result<Vec<String>, Error>`: List of valid index page paths
pub async fn fetch_pypi_index_cached(
    client: &Client,
    url: &str,
    cache: &mut PypiIndexCache,
    config: &PypiIndexConfig,
) -> Result<Vec<String>, Error> {
    // Get the root page (always fetch root to check for changes)
    let root_resp = client.get(url).send().await?;
    let root_url = root_resp.url().clone();

    let root_etag = root_resp
        .headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let root_last_modified = root_resp
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let root_html = root_resp.text().await?;
    let root_links = parse_pypi_index(&root_html);

    // Cache the root page
    cache.insert(
        String::new(),
        CachedPage {
            etag: root_etag,
            last_modified: root_last_modified,
            links: root_links.clone(),
        },
    );

    // BFS queue: (relative_path, page_url, depth)
    // We process links from cached data, only fetching when needed
    let mut queue: VecDeque<(String, Url, usize)> = VecDeque::new();
    let mut seen_pages: HashSet<String> = HashSet::from([String::new()]);
    let mut pages_fetched: usize = 1; // root already fetched

    // Seed queue with links from root page
    for link in &root_links {
        if is_largefile_link(link) {
            continue;
        }
        if let Some((rel_path, abs_url)) = resolve_child_url(&root_url, &root_url, link)
            && seen_pages.insert(rel_path.clone())
        {
            queue.push_back((rel_path, abs_url, 1));
        }
    }

    // BFS traversal with depth and page limits
    while let Some((relative_path, page_url, depth)) = queue.pop_front() {
        // Enforce depth limit
        if depth > config.max_depth {
            debug!(
                "Skipping {} - exceeds max depth {}",
                relative_path, config.max_depth
            );
            continue;
        }

        // Enforce page limit
        if pages_fetched >= config.max_pages {
            debug!(
                "Reached max pages limit {}, stopping crawl",
                config.max_pages
            );
            break;
        }

        // Fetch with conditional GET if we have cached data
        let cached = cache.get(&relative_path);
        let fetch_result = fetch_page_conditional(client, &page_url, cached).await;

        let links = match fetch_result {
            FetchResult::Fetched {
                html,
                etag,
                last_modified,
            } => {
                pages_fetched += 1;
                let links = parse_pypi_index(&html);
                cache.insert(
                    relative_path.clone(),
                    CachedPage {
                        etag,
                        last_modified,
                        links: links.clone(),
                    },
                );
                links
            }
            FetchResult::NotModified => {
                // Use cached links, no need to re-parse
                if let Some(cached) = cache.get(&relative_path) {
                    cached.links.clone()
                } else {
                    continue;
                }
            }
            FetchResult::Error(e) => {
                warn!("Failed to fetch {}: {:?}", relative_path, e);
                continue;
            }
        };

        // Queue child links if we haven't reached max depth
        if depth < config.max_depth {
            for link in &links {
                if is_largefile_link(link) {
                    continue;
                }
                if let Some((rel_path, abs_url)) = resolve_child_url(&root_url, &page_url, link)
                    && seen_pages.insert(rel_path.clone())
                {
                    queue.push_back((rel_path, abs_url, depth + 1));
                }
            }
        }
    }

    debug!(
        "Index crawl complete: {} pages fetched, {} pages cached",
        pages_fetched,
        cache.pages.len()
    );

    Ok(cache.valid_pages())
}

// pytorch-wheels

pub struct PypiIndexState {
    pub entries: RwLock<Vec<String>>, // Sorted list of relative paths to index pages.
    worker_started: AtomicBool,       // One-time flag for background worker spawning.
}

impl Default for PypiIndexState {
    fn default() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            worker_started: AtomicBool::new(false),
        }
    }
}

fn normalize_pypi_index(mut index: Vec<String>) -> Vec<String> {
    index.retain(|entry| !entry.is_empty());
    index.sort();
    index.dedup();
    index
}

fn apply_pypi_index_update(index: Vec<String>, index_state: &'static PypiIndexState) {
    // Replace in-memory index with the latest upstream snapshot.
    let mut entries = index_state
        .entries
        .write()
        .expect("PyPI index lock poisoned");

    if *entries == index {
        return;
    }

    *entries = index;
}

pub fn schedule_wheels_index_worker(config: &Config, index_state: &'static PypiIndexState) {
    if index_state
        .worker_started
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    let endpoint = config.endpoints.pytorch_wheels.clone();
    let pypi_config = config.pypi_index.clone();
    let timeout_secs = config.download_timeout.min(pypi_config.fetch_timeout_secs);
    let client = match reqwest::ClientBuilder::new()
        .user_agent(&config.user_agent)
        .timeout(Duration::from_secs(timeout_secs))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            warn!("Failed to create PyPI index client: {:?}", err);
            index_state.worker_started.store(false, Ordering::Release);
            return;
        }
    };

    tokio::spawn(async move {
        let mut refresh = tokio::time::interval(Duration::from_secs(pypi_config.refresh_secs));
        refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Persistent cache for conditional requests across refresh cycles
        let mut cache = PypiIndexCache::new();

        loop {
            refresh.tick().await;
            match fetch_pypi_index_cached(&client, &endpoint, &mut cache, &pypi_config).await {
                Ok(index) => {
                    let normalized = normalize_pypi_index(index);
                    apply_pypi_index_update(normalized, index_state);
                }
                Err(err) => {
                    warn!("Failed to fetch PyPI index for pytorch-wheels: {:?}", err);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::LazyLock;

    #[test]
    fn test_resolve_child_url() {
        let root = Url::parse("https://download.pytorch.org/whl").unwrap();
        let parent = Url::parse("https://download.pytorch.org/whl/torch/").unwrap();
        let link = "../cu130/torch/";
        let Some((relative, child)) = resolve_child_url(&root, &parent, link) else {
            panic!("Failed to resolve child URL");
        };
        assert_eq!(relative, "cu130/torch");
        assert_eq!(
            child.as_str(),
            "https://download.pytorch.org/whl/cu130/torch/"
        );
    }

    #[test]
    fn test_resolve_child_url_parent_without_trailing_slash() {
        let root = Url::parse("https://download.pytorch.org/whl").unwrap();
        let parent = Url::parse("https://download.pytorch.org/whl/torch").unwrap();
        let link = "../cu130/torch/";
        let Some((relative, child)) = resolve_child_url(&root, &parent, link) else {
            panic!("Failed to resolve child URL");
        };
        assert_eq!(relative, "cu130/torch");
        assert_eq!(
            child.as_str(),
            "https://download.pytorch.org/whl/cu130/torch/"
        );
    }

    #[test]
    fn test_resolve_child_url_parent_with_trailing_slash() {
        let root = Url::parse("https://download.pytorch.org/whl").unwrap();
        let parent = Url::parse("https://download.pytorch.org/whl/torch/").unwrap();
        let link = "../cu130/torch/";
        let Some((relative, child)) = resolve_child_url(&root, &parent, link) else {
            panic!("Failed to resolve child URL");
        };
        assert_eq!(relative, "cu130/torch");
        assert_eq!(
            child.as_str(),
            "https://download.pytorch.org/whl/cu130/torch/"
        );
    }

    #[test]
    fn test_resolve_child_url_reject_root_link() {
        let root = Url::parse("https://download.pytorch.org/whl").unwrap();
        let parent = Url::parse("https://download.pytorch.org/whl").unwrap();
        let link = "./";
        let resolved = resolve_child_url(&root, &parent, link);
        assert!(resolved.is_none());
    }

    #[test]
    fn test_resolve_child_url_reject_cross_origin() {
        let root = Url::parse("https://download.pytorch.org/whl").unwrap();
        let parent = Url::parse("https://download.pytorch.org/whl").unwrap();
        let link = "https://example.com/torch/";
        let resolved = resolve_child_url(&root, &parent, link);
        assert!(resolved.is_none());
    }

    static ROOT_FIXTURE: LazyLock<String> = LazyLock::new(|| {
        r#"
        <!DOCTYPE html>
        <html>
        <body>
            <a href="torch/">torch</a><br/>
            <a href="cu130/">cu130</a><br/>
        </body>
        </html>
        "#
        .to_string()
    });

    static CU130_FIXTURE: LazyLock<String> = LazyLock::new(|| {
        r#"
        <!DOCTYPE html>
        <html>
        <body>
            <a href="torch/">torch</a><br/>
        </body>
        </html>
        "#
        .to_string()
    });

    #[test]
    fn test_parse_pypi_index() {
        const WHL: &str = include_str!("../tests/pytorch_wheels/whl.html");
        let links = parse_pypi_index(WHL);
        println!("{:?}", links);

        let expected: HashSet<&str> = [
            "torch/",
            "torchaudio/",
            "torchvision/",
            "triton/",
            "cu130/",
            "rocm7.2/",
        ]
        .into_iter()
        .collect();

        let actual: HashSet<&str> = links.iter().map(|link| link.as_str()).collect();

        assert!(expected.is_subset(&actual));
    }

    #[tokio::test]
    async fn test_fetch_pypi_index() {
        use httpmock::Method::GET;
        use httpmock::MockServer;

        const WHL_TORCH: &str = include_str!("../tests/pytorch_wheels/whl_torch.html");
        const WHL_CU130_TORCH: &str = include_str!("../tests/pytorch_wheels/whl_cu130_torch.html");

        let server = MockServer::start_async().await;
        let root = server
            .mock_async(|when, then| {
                when.method(GET).path("/whl");
                then.status(200).body(ROOT_FIXTURE.as_str());
            })
            .await;
        let torch = server
            .mock_async(|when, then| {
                when.method(GET).path("/whl/torch/");
                then.status(200).body(WHL_TORCH);
            })
            .await;
        let cu130 = server
            .mock_async(|when, then| {
                when.method(GET).path("/whl/cu130/");
                then.status(200).body(CU130_FIXTURE.as_str());
            })
            .await;
        let cu130_torch = server
            .mock_async(|when, then| {
                when.method(GET).path("/whl/cu130/torch/");
                then.status(200).body(WHL_CU130_TORCH);
            })
            .await;

        let client = Client::new();
        let index_pages = fetch_pypi_index_cached(
            &client,
            &(server.base_url() + "/whl"),
            &mut PypiIndexCache::new(),
            &PypiIndexConfig::default(),
        )
        .await
        .unwrap();

        let expected: HashSet<&str> = ["torch", "cu130", "cu130/torch"].into_iter().collect();
        let actual: HashSet<&str> = index_pages.iter().map(|s| s.as_str()).collect();

        assert_eq!(expected, actual);

        root.assert_calls_async(1).await;
        torch.assert_calls_async(1).await;
        cu130.assert_calls_async(1).await;
        cu130_torch.assert_calls_async(1).await;
    }

    #[test]
    fn test_apply_pypi_index_update_replaces_entries() {
        let index_state: &'static PypiIndexState = Box::leak(Box::new(PypiIndexState::default()));

        apply_pypi_index_update(vec!["cu130".to_string(), "torch".to_string()], index_state);
        apply_pypi_index_update(vec!["torch".to_string()], index_state);

        let entries = index_state
            .entries
            .read()
            .expect("PyPI index lock poisoned");
        assert_eq!(entries.as_slice(), ["torch"]);
    }

    #[tokio::test]
    async fn test_fetch_pypi_index_cached_respects_depth_limit() {
        use httpmock::Method::GET;
        use httpmock::MockServer;

        // depth 3 content that should NOT be fetched with max_depth=2
        const DEEP_PAGE: &str =
            r#"<!DOCTYPE html><html><body><a href="deep.whl">deep.whl</a></body></html>"#;

        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/whl");
                then.status(200).body(ROOT_FIXTURE.as_str());
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/whl/torch/");
                then.status(200).body(
                    r#"<!DOCTYPE html><html><body><a href="file.whl">file</a></body></html>"#,
                );
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/whl/cu130/");
                then.status(200).body(CU130_FIXTURE.as_str());
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/whl/cu130/torch/");
                // Has a subdir link
                then.status(200).body(
                    r#"<!DOCTYPE html><html><body><a href="subdir/">subdir</a></body></html>"#,
                );
            })
            .await;
        let deep_mock = server
            .mock_async(|when, then| {
                // This is depth 3 - should NOT be fetched with max_depth=2
                when.method(GET).path("/whl/cu130/torch/subdir/");
                then.status(200).body(DEEP_PAGE);
            })
            .await;

        let client = Client::new();
        let mut cache = PypiIndexCache::new();
        let config = PypiIndexConfig {
            max_depth: 2,
            max_pages: 1000,
            ..Default::default()
        };

        let _pages =
            fetch_pypi_index_cached(&client, &(server.base_url() + "/whl"), &mut cache, &config)
                .await
                .unwrap();

        // Verify the deep page was NOT fetched
        deep_mock.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn test_fetch_pypi_index_cached_conditional_request() {
        use httpmock::Method::GET;
        use httpmock::MockServer;

        let server = MockServer::start_async().await;

        // First request returns content with ETag
        let root_mock = server
            .mock_async(|when, then| {
                when.method(GET).path("/whl");
                then.status(200)
                    .header("ETag", "\"abc123\"")
                    .body(r#"<!DOCTYPE html><html><body><a href="pkg/">pkg</a></body></html>"#);
            })
            .await;

        let pkg_mock = server
            .mock_async(|when, then| {
                when.method(GET).path("/whl/pkg/");
                then.status(200).header("ETag", "\"pkg456\"").body(
                    r#"<!DOCTYPE html><html><body><a href="file.whl">file</a></body></html>"#,
                );
            })
            .await;

        let client = Client::new();
        let mut cache = PypiIndexCache::new();
        let config = PypiIndexConfig::default();

        // First fetch - populates cache
        let pages =
            fetch_pypi_index_cached(&client, &(server.base_url() + "/whl"), &mut cache, &config)
                .await
                .unwrap();

        assert_eq!(pages, vec!["pkg"]);
        root_mock.assert_calls_async(1).await;
        pkg_mock.assert_calls_async(1).await;

        // Verify cache has ETag stored
        let cached_pkg = cache.get("pkg").unwrap();
        assert_eq!(cached_pkg.etag, Some("\"pkg456\"".to_string()));
    }

    #[tokio::test]
    async fn test_fetch_pypi_index_cached_304_not_modified() {
        use httpmock::Method::GET;
        use httpmock::MockServer;

        let server = MockServer::start_async().await;

        // Create mocks that return 304 for conditional requests
        server
            .mock_async(|when, then| {
                when.method(GET).path("/whl");
                then.status(200)
                    .header("ETag", "\"root-etag\"")
                    .body(r#"<!DOCTYPE html><html><body><a href="pkg/">pkg</a></body></html>"#);
            })
            .await;

        // For pkg/, first return 200, then check for conditional header
        let pkg_initial = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/whl/pkg/")
                    .header_missing("If-None-Match");
                then.status(200).header("ETag", "\"pkg-etag\"").body(
                    r#"<!DOCTYPE html><html><body><a href="file.whl">file</a></body></html>"#,
                );
            })
            .await;

        let pkg_conditional = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/whl/pkg/")
                    .header("If-None-Match", "\"pkg-etag\"");
                then.status(304); // Not Modified
            })
            .await;

        let client = Client::new();
        let mut cache = PypiIndexCache::new();
        let config = PypiIndexConfig::default();

        // First fetch
        fetch_pypi_index_cached(&client, &(server.base_url() + "/whl"), &mut cache, &config)
            .await
            .unwrap();

        pkg_initial.assert_calls_async(1).await;
        pkg_conditional.assert_calls_async(0).await;

        // Second fetch - should use conditional request
        let pages =
            fetch_pypi_index_cached(&client, &(server.base_url() + "/whl"), &mut cache, &config)
                .await
                .unwrap();

        assert_eq!(pages, vec!["pkg"]);
        pkg_conditional.assert_calls_async(1).await;
    }

    #[test]
    fn test_pypi_index_cache_valid_pages() {
        let mut cache = PypiIndexCache::new();

        // Empty path should not be included
        cache.insert(
            String::new(),
            CachedPage {
                etag: None,
                last_modified: None,
                links: vec!["something".to_string()],
            },
        );

        // Path with links should be included
        cache.insert(
            "torch".to_string(),
            CachedPage {
                etag: Some("etag".to_string()),
                last_modified: None,
                links: vec!["file.whl".to_string()],
            },
        );

        // Path with no links should not be included
        cache.insert(
            "empty".to_string(),
            CachedPage {
                etag: None,
                last_modified: None,
                links: vec![],
            },
        );

        let valid = cache.valid_pages();
        assert_eq!(valid, vec!["torch"]);
    }
}
