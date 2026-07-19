use crate::cli::{AppConfig, log_debug, log_info};
use crate::errors::FinalStatus;
use crate::fetcher::{self, FileFetch, ListingFetch, ResumeHints, RetryConfig};
use crate::manifest::ManifestStore;
use crate::models::{DownloadTask, Manifest, ManifestEntry, Stats};
use crate::parser::parse_listing_entries;
use crate::store::write_atomic;
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use globset::{Glob, GlobSet, GlobSetBuilder};
use reqwest::Client;
use reqwest::redirect::Policy;
use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use url::Url;

/// Records between manifest checkpoints. Bounds how much progress a crash can
/// erase. ponytail: ceiling = time-based checkpoint (flush every N seconds) if
/// the per-record counter ever proves too coarse for huge files.
const CHECKPOINT_INTERVAL: usize = 50;

pub async fn run(config: &AppConfig) -> Result<FinalStatus> {
    if !config.dry_run {
        fs::create_dir_all(&config.output).await.with_context(|| {
            format!(
                "failed to create output directory {}",
                config.output.display()
            )
        })?;
    }

    let manifest_path = resolve_manifest_path(config);
    let manifest = if config.dry_run {
        // dry-run never writes; an in-memory store keeps the resume logic honest
        // without touching disk.
        ManifestStore::for_memory()
    } else {
        ManifestStore::load(&manifest_path).await?
    };

    let matcher = build_matcher(&config.includes, &config.excludes)?;
    let retry = RetryConfig {
        retries: config.retries,
        retry_backoff_ms: config.retry_backoff_ms,
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(config.timeout_seconds))
        .user_agent(config.user_agent.clone())
        .redirect(Policy::limited(config.max_redirects))
        .build()
        .context("failed to create HTTP client")?;

    let cfg = Arc::new(config.clone());
    let matcher = Arc::new(matcher);
    let client = Arc::new(client);
    let manifest = Arc::new(manifest);

    // SIGINT/SIGTERM handler: flush the manifest once, then re-raise so the
    // process actually exits. A best-effort flush — if it errors we still exit.
    let sig_manifest = manifest.clone();
    let sig_path = manifest_path.clone();
    let stop = {
        let log_level = config.log_level;
        tokio::task::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                log_info(log_level, "Interrupted - flushing manifest…");
                if !sig_manifest.is_dry_run() {
                    let _ = sig_manifest.checkpoint().await;
                }
                log_info(
                    log_level,
                    &format!("Manifest flushed to {}", sig_path.display()),
                );
                // Re-raise as an interrupt-style exit (128 + SIGINT = 130).
                std::process::exit(130);
            }
        })
    };

    let mut visited = HashSet::new();
    let mut queue = vec![config.root_url.clone()];
    let mut stats = Stats::default();
    let mut since_checkpoint: usize = 0;

    let concurrency = config.concurrency.max(1);

    while !queue.is_empty() {
        // Fetch every directory currently in the queue concurrently (bounded by
        // the shared concurrency budget). This is the throughput fix for defect
        // #1: directory traversal is now parallel, not serial.
        let listings = stream::iter(queue.drain(..))
            .map(|url| {
                let client = client.clone();
                async move {
                    let out = fetcher::fetch_listing(&client, &url, retry, config.log_level).await;
                    (url, out)
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut file_tasks: Vec<DownloadTask> = Vec::new();
        for (current_url, listing) in listings {
            let normalized = normalize_url(&current_url);
            if !visited.insert(normalized) {
                continue;
            }

            let Some(relative_dir) = relative_path(&config.root_url, &current_url) else {
                log_info(
                    config.log_level,
                    &format!("Skipping outside-scope URL: {current_url}"),
                );
                continue;
            };

            match listing {
                ListingFetch::Body(body) => {
                    if !config.dry_run {
                        let local_dir = config.output.join(&relative_dir);
                        fs::create_dir_all(&local_dir).await.with_context(|| {
                            format!("failed to create directory {}", local_dir.display())
                        })?;
                    }
                    stats.dirs_processed += 1;
                    let mut entries = parse_listing_entries(&body, &current_url);
                    entries.sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));

                    for entry in entries {
                        if !is_under_root(&config.root_url, &entry.url) {
                            continue;
                        }
                        let Some(rel_path) = relative_path(&config.root_url, &entry.url) else {
                            continue;
                        };
                        if entry.is_dir {
                            queue.push(ensure_trailing_slash(entry.url));
                            continue;
                        }
                        if !matcher.is_allowed(&rel_path) {
                            stats.files_skipped += 1;
                            log_debug(
                                config.log_level,
                                &format!("Filtered out {}", rel_path.to_string_lossy()),
                            );
                            continue;
                        }
                        file_tasks.push(DownloadTask {
                            file_url: entry.url,
                            relative_path: rel_path,
                        });
                    }
                }
                ListingFetch::Skipped => stats.files_skipped += 1,
                ListingFetch::Failed => stats.files_failed += 1,
            }
        }

        if file_tasks.is_empty() {
            continue;
        }

        let outcomes = stream::iter(file_tasks)
            .map(|task| {
                let cfg = cfg.clone();
                let client = client.clone();
                let manifest = manifest.clone();
                async move { process_single_task(&client, &cfg, &manifest, retry, task).await }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let downloaded = apply_outcomes(&manifest, &mut stats, outcomes).await;

        // Checkpoint periodically so a crash erases at most CHECKPOINT_INTERVAL
        // records of progress (defect #2: previously written only at the end).
        since_checkpoint += downloaded;
        if !config.dry_run && since_checkpoint >= CHECKPOINT_INTERVAL {
            if let Err(err) = manifest.checkpoint().await {
                log_info(
                    config.log_level,
                    &format!("Manifest checkpoint failed: {err:#}"),
                );
                stats.warnings += 1;
            }
            since_checkpoint = 0;
        }
    }

    // Final flush.
    if !config.dry_run {
        manifest
            .checkpoint()
            .await
            .with_context(|| format!("failed to write manifest {}", manifest_path.display()))?;
    }

    stop.abort();
    stats.summarize();
    Ok(stats.final_status())
}

#[derive(Debug)]
enum TaskOutcome {
    Downloaded { url: Url, entry: ManifestEntry },
    Skipped,
    Failed,
}

async fn process_single_task(
    client: &Client,
    config: &AppConfig,
    manifest: &ManifestStore,
    retry: RetryConfig,
    task: DownloadTask,
) -> TaskOutcome {
    let output_path = config.output.join(&task.relative_path);

    let (resume, already_current) = {
        let m = manifest.lock().await;
        resume_hit(config, &m, &task.file_url, &output_path)
    };
    if already_current {
        log_debug(
            config.log_level,
            &format!("Resume skip for {}", task.file_url),
        );
        return TaskOutcome::Skipped;
    }

    let fetched =
        fetcher::fetch_file(client, &task.file_url, &resume, retry, config.log_level).await;
    match fetched {
        FileFetch::Downloaded(payload) => {
            if config.dry_run {
                log_info(
                    config.log_level,
                    &format!("[dry-run] Would download {}", task.file_url),
                );
                return TaskOutcome::Skipped;
            }
            if let Err(err) = write_atomic(&output_path, &payload.bytes).await {
                log_info(
                    config.log_level,
                    &format!("Failed to write file {}: {err}", output_path.display()),
                );
                return TaskOutcome::Failed;
            }
            TaskOutcome::Downloaded {
                url: task.file_url.clone(),
                entry: ManifestEntry {
                    local_path: normalize_path_string(&task.relative_path),
                    size: payload.size,
                    etag: payload.etag,
                    last_modified: payload.last_modified,
                },
            }
        }
        FileFetch::NotModified => {
            // Server confirmed the on-disk file is current; nothing to write.
            log_debug(
                config.log_level,
                &format!("304 Not Modified for {}", task.file_url),
            );
            TaskOutcome::Skipped
        }
        FileFetch::Skipped => TaskOutcome::Skipped,
        FileFetch::Failed => TaskOutcome::Failed,
    }
}

/// Apply outcomes to the manifest store; returns the number newly downloaded so
/// the caller can drive checkpoint cadence.
async fn apply_outcomes(
    manifest: &ManifestStore,
    stats: &mut Stats,
    outcomes: Vec<TaskOutcome>,
) -> usize {
    let mut downloaded = 0usize;
    for outcome in outcomes {
        match outcome {
            TaskOutcome::Downloaded { url, entry } => {
                manifest.record(normalize_url(&url), entry).await;
                stats.files_downloaded += 1;
                downloaded += 1;
            }
            TaskOutcome::Skipped => stats.files_skipped += 1,
            TaskOutcome::Failed => stats.files_failed += 1,
        }
    }
    downloaded
}

#[derive(Debug)]
struct Matcher {
    include: Option<GlobSet>,
    exclude: Option<GlobSet>,
}

impl Matcher {
    fn is_allowed(&self, relative_path: &Path) -> bool {
        let target = normalize_path_string(relative_path);

        let included = self
            .include
            .as_ref()
            .map(|set| set.is_match(&target))
            .unwrap_or(true);
        if !included {
            return false;
        }

        let excluded = self
            .exclude
            .as_ref()
            .map(|set| set.is_match(&target))
            .unwrap_or(false);
        !excluded
    }
}

fn build_matcher(includes: &[String], excludes: &[String]) -> Result<Matcher> {
    Ok(Matcher {
        include: build_globset(includes)?,
        exclude: build_globset(excludes)?,
    })
}

fn build_globset(patterns: &[String]) -> Result<Option<GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }

    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder
            .add(Glob::new(pattern).with_context(|| format!("invalid glob pattern: {pattern}"))?);
    }

    let set = builder.build().context("failed to build globset")?;
    Ok(Some(set))
}

/// Returns (conditional-GET hints, already_current) for a file. `already_current`
/// is true when the manifest + on-disk file agree and no re-fetch is needed.
fn resume_hit(
    config: &AppConfig,
    manifest: &Manifest,
    url: &Url,
    output_path: &Path,
) -> (ResumeHints, bool) {
    let entry = manifest.files.get(&normalize_url(url));
    let hints = match entry {
        Some(e) => ResumeHints {
            etag: e.etag.clone(),
            last_modified: e.last_modified.clone(),
        },
        None => ResumeHints::default(),
    };

    if config.force {
        return (hints, false);
    }

    let Some(entry) = entry else {
        return (hints, false);
    };
    if !output_path.exists() {
        return (hints, false);
    }

    // If we have conditional headers, issue a conditional GET and let the server
    // decide (304 = current, 200 = changed). We can't short-circuit here.
    if !hints.is_empty() {
        return (hints, false);
    }

    // No conditional headers: fall back to the prior size-match heuristic so
    // resume still works against servers that omit ETag/Last-Modified.
    let same_path = entry.local_path
        == normalize_path_string(&relative_from_output(&config.output, output_path));
    let same_size = std::fs::metadata(output_path)
        .map(|meta| meta.len() == entry.size)
        .unwrap_or(false);
    (hints, same_path && same_size)
}

fn relative_from_output(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root).unwrap_or(path).to_path_buf()
}

fn resolve_manifest_path(config: &AppConfig) -> PathBuf {
    if config.manifest.is_absolute() {
        config.manifest.clone()
    } else {
        config.output.join(&config.manifest)
    }
}

fn normalize_url(url: &Url) -> String {
    let mut clone = url.clone();
    clone.set_fragment(None);
    clone.set_query(None);
    clone.to_string()
}

fn is_under_root(root_url: &Url, url: &Url) -> bool {
    root_url.scheme() == url.scheme()
        && root_url.domain() == url.domain()
        && root_url.port_or_known_default() == url.port_or_known_default()
        && url.path().starts_with(root_url.path())
}

fn relative_path(root_url: &Url, target_url: &Url) -> Option<PathBuf> {
    if !is_under_root(root_url, target_url) {
        return None;
    }

    let root_path = root_url.path();
    let target_path = target_url.path();
    let relative = target_path.strip_prefix(root_path)?;

    let cleaned = relative.trim_start_matches('/');
    let candidate = if cleaned.is_empty() {
        PathBuf::new()
    } else {
        PathBuf::from(cleaned)
    };

    if is_safe_relative_path(&candidate) {
        Some(candidate)
    } else {
        None
    }
}

fn is_safe_relative_path(path: &Path) -> bool {
    !path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    })
}

fn ensure_trailing_slash(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    url
}

fn normalize_path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_path_outside_scope_is_none() {
        let root = Url::parse("http://example.com/root/").unwrap();
        let other = Url::parse("http://example.com/another/file.txt").unwrap();
        assert!(relative_path(&root, &other).is_none());
    }

    #[test]
    fn matcher_respects_include_exclude() {
        let matcher = build_matcher(&["**/*.txt".to_string()], &["secret*".to_string()]).unwrap();
        assert!(matcher.is_allowed(Path::new("ok/file.txt")));
        assert!(!matcher.is_allowed(Path::new("secret.txt")));
        assert!(!matcher.is_allowed(Path::new("file.bin")));
    }
}
