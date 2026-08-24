//! Metrics for mirror-intel's on-disk temporary cache.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::common::Metrics;

/// Periodically refresh the temporary cache directory metrics.
pub async fn monitor_cache_size(path: PathBuf, interval: Duration, metrics: Arc<Metrics>) {
    let interval = interval.max(Duration::from_secs(1));
    loop {
        refresh_cache_size(path.clone(), metrics.clone()).await;
        sleep(interval).await;
    }
}

async fn refresh_cache_size(path: PathBuf, metrics: Arc<Metrics>) {
    match task::spawn_blocking(move || directory_size(&path)).await {
        Ok(Ok(size)) => {
            let size = i64::try_from(size).unwrap_or(i64::MAX);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let timestamp = i64::try_from(timestamp).unwrap_or(i64::MAX);

            metrics.mirror_intel_cache_size_bytes.set(size);
            metrics.mirror_intel_cache_size_scan_success.set(1);
            metrics
                .mirror_intel_cache_size_scan_timestamp_seconds
                .set(timestamp);
            debug!(
                path_size_bytes = size,
                "temporary cache size scan completed"
            );
        }
        Ok(Err(error)) => {
            metrics.mirror_intel_cache_size_scan_success.set(0);
            warn!(?error, "temporary cache size scan failed");
        }
        Err(error) => {
            metrics.mirror_intel_cache_size_scan_success.set(0);
            warn!(?error, "temporary cache size scan task failed");
        }
    }
}

/// Return the apparent size of regular files below `root`.
///
/// Symlinks are deliberately skipped so a cache entry cannot make the scanner
/// leave the configured directory. Temporary cache files are not hard-linked,
/// so summing file lengths matches the storage mirror-intel controls.
fn directory_size(root: &Path) -> io::Result<u64> {
    let mut size = 0_u64;
    let mut directories = vec![root.to_path_buf()];

    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() {
                size = size.saturating_add(entry.metadata()?.len());
            }
        }
    }

    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn must_sum_nested_regular_files() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("one"), b"abc").unwrap();
        fs::create_dir(root.path().join("nested")).unwrap();
        fs::write(root.path().join("nested/two"), b"12345").unwrap();

        assert_eq!(directory_size(root.path()).unwrap(), 8);
    }

    #[cfg(unix)]
    #[test]
    fn must_not_follow_symlinks() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("large"), vec![0_u8; 4096]).unwrap();
        symlink(outside.path(), root.path().join("outside")).unwrap();

        assert_eq!(directory_size(root.path()).unwrap(), 0);
    }

    #[tokio::test]
    async fn must_publish_successful_scan() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cache"), b"12345").unwrap();
        let metrics = Arc::new(Metrics::default());

        refresh_cache_size(root.path().to_path_buf(), metrics.clone()).await;

        assert_eq!(metrics.mirror_intel_cache_size_bytes.get(), 5);
        assert_eq!(metrics.mirror_intel_cache_size_scan_success.get(), 1);
        assert!(metrics.mirror_intel_cache_size_scan_timestamp_seconds.get() > 0);
    }

    #[tokio::test]
    async fn failed_scan_must_preserve_last_size_and_timestamp() {
        let metrics = Arc::new(Metrics::default());
        metrics.mirror_intel_cache_size_bytes.set(123);
        metrics
            .mirror_intel_cache_size_scan_timestamp_seconds
            .set(456);

        refresh_cache_size(PathBuf::from("/path/that/does/not/exist"), metrics.clone()).await;

        assert_eq!(metrics.mirror_intel_cache_size_bytes.get(), 123);
        assert_eq!(metrics.mirror_intel_cache_size_scan_success.get(), 0);
        assert_eq!(
            metrics.mirror_intel_cache_size_scan_timestamp_seconds.get(),
            456
        );
    }
}
