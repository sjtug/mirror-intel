use reqwest::Client;
use scraper::{Html, Selector};
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    collections::{BTreeSet, HashSet, VecDeque},
    sync::{RwLock, atomic::AtomicBool},
};
use tokio::time::MissedTickBehavior;
use tracing::warn;
use url::Url;

use crate::error::Error;

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

/// From a PyPI index url (e.g. <https://download.pytorch.org/whl>),
/// fetch the HTML content under root path, parse via `parse_pypi_index`,
/// and recursively fetch the HTML content of each link in the page,
/// until the parsed content contains links to only .whl files.
///
/// # Arguments
///  * `url`: The root PyPI index URL.
///
/// # Return
///  * `Result<Vec<String>, Error>`: A list of all relative paths that are valid index pages.
pub async fn fetch_pypi_index(url: &str) -> Result<Vec<String>, Error> {
    let client = Client::new();

    // Get the root page content and final URL of request
    let root_resp = client.get(url).send().await?;
    let root_url = root_resp.url().clone();
    let root_html = root_resp.text().await?;

    // Initialize BFS queue containing (relative_path, page_url, html), and track seen pages to avoid cycles.
    let mut queue = VecDeque::from([(String::new(), root_url.clone(), root_html)]);
    let mut seen_pages = HashSet::from([String::new()]);
    let mut valid_pages = BTreeSet::new();

    // BFS traversal of index pages
    while let Some((relative_path, page_url, html)) = queue.pop_front() {
        // Parse the page content to extract href links.
        let links = parse_pypi_index(&html);

        // Add to valid pages if it contains valid links
        if !links.is_empty() && !relative_path.is_empty() {
            valid_pages.insert(relative_path.clone());
        }

        for link in links {
            // Skip links that point to large files.
            if is_largefile_link(&link) {
                continue;
            }

            // Resolve the link to an absolute URL, and get the path relative to root URL.
            let Some((next_relative_path, next_url)) =
                resolve_child_url(&root_url, &page_url, &link)
            else {
                continue;
            };

            // Skip if we've already seen this page before.
            if !seen_pages.insert(next_relative_path.clone()) {
                continue;
            }

            // Fetch the page content and add to queue for further processing.
            let next_html = client.get(next_url.clone()).send().await?.text().await?;
            queue.push_back((next_relative_path, next_url, next_html));
        }
    }

    // Return the valid index pages, in lexicographical order
    Ok(valid_pages.into_iter().collect::<Vec<_>>())
}

// pytorch-wheels

const PYPI_INDEX_REFRESH_SECS: u64 = 300;

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
    // Incremental update: only append newly discovered pages to in-memory index.
    let mut entries = index_state
        .entries
        .write()
        .expect("PyPI index lock poisoned");
    let mut merged = entries.clone();
    merged.extend(index);
    let merged = normalize_pypi_index(merged);

    if merged == *entries {
        return;
    }

    *entries = merged;
}

pub fn schedule_wheels_index_worker(endpoint: String, index_state: &'static PypiIndexState) {
    if index_state
        .worker_started
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    tokio::spawn(async move {
        let mut refresh = tokio::time::interval(Duration::from_secs(PYPI_INDEX_REFRESH_SECS));
        refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            refresh.tick().await;
            match fetch_pypi_index(&endpoint).await {
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

        let index_pages = fetch_pypi_index(&(server.base_url() + "/whl"))
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
}
