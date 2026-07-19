use crate::cli::{AppConfig, log_debug, log_info};
use crate::errors::FinalStatus;
use crate::fetcher::{self, FileFetch, ListingFetch, ResumeHints, RetryConfig};
use crate::manifest::ManifestStore;
use crate::models::{DownloadTask, EntrySource, Manifest, ManifestEntry, Stats};
use crate::parser::parse_listing_entries;
use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use reqwest::Client;
use reqwest::redirect::Policy;
use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use url::Url;

/// Records between manifest checkpoints. Bounds how much progress a crash can
/// erase.
const CHECKPOINT_INTERVAL: usize = 50;

/// Work item in the unified queue. Listings and file downloads share the same
/// worker pool so a slow listing doesn't block file transfers that are ready
/// to run — the previous per-level barrier was the biggest wall-clock waster
/// on deeply-nested targets.
#[derive(Debug)]
enum Work {
    /// Depth is 0 at the root listing and increases by 1 for each descent.
    /// Files inherit their parent listing's depth for logging; only listing
    /// depth is used against the `--depth` cap.
    Listing { url: Url, depth: u32 },
    File(DownloadTask),
}

pub async fn run(config: &AppConfig) -> Result<FinalStatus> {
    // Progress bar is retrieved from the process-wide slot when installed by
    // `lib::execute`; a headless singleton is used otherwise so counter calls
    // are cheap no-ops in tests and library consumers.
    let progress = crate::progress::active();
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
        // Wall-clock cap for the entire request. Default is 0 (disabled) so a
        // legitimately-slow multi-hundred-MB file at slow-server-speeds isn't
        // killed halfway. If the user asks for a positive cap we honour it.
        .connect_timeout(Duration::from_secs(config.connect_timeout))
        // Per-read idle timeout: resets every time bytes arrive. This is what
        // actually catches stalled connections without punishing slow-but-live
        // ones. Root fix for the "Stream error / error decoding response body"
        // symptom that appeared on large files under the previous wall-clock
        // timeout (a 200 MB body at 300 KB/s legitimately takes 11 minutes
        // and used to fail at 60s).
        .read_timeout(Duration::from_secs(config.read_timeout))
        .user_agent(config.user_agent.clone())
        .redirect(Policy::limited(config.max_redirects))
        // Compression: HTML listings compress ~10× and hermes-style .git
        // exposures include lots of text objects. Wire savings dwarf the CPU
        // cost of decoding.
        .gzip(true)
        .deflate(true)
        .brotli(true)
        // Connection reuse: with concurrency ≥ 16 we do dozens of requests
        // to the same origin. The default (idle-pool of 32 per host) is
        // already fine; we set it explicitly to make the intent visible and
        // to keep the pool warm for the crawl's duration.
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true);
    let client = if config.timeout_seconds > 0 {
        client.timeout(Duration::from_secs(config.timeout_seconds))
    } else {
        client
    };
    let client = client
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

    let visited: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let stats = Arc::new(Mutex::new(Stats::default()));
    let concurrency = config.concurrency.max(1);
    let sem = Arc::new(Semaphore::new(concurrency));

    // Unbounded channel keeps producers (workers) non-blocking; the semaphore
    // above is what actually bounds parallelism. `pending` tracks outstanding
    // work items — when it hits zero we know the crawl is done and can drop
    // every sender clone, which lets the receiver's `.recv()` return None.
    let (tx, mut rx) = mpsc::unbounded_channel::<Work>();
    let pending = Arc::new(AtomicUsize::new(1));
    let done_notify = Arc::new(Notify::new());
    let downloaded_since_ckpt = Arc::new(AtomicUsize::new(0));

    // Seed the queue with the root listing at depth 0.
    tx.send(Work::Listing {
        url: config.root_url.clone(),
        depth: 0,
    })
    .expect("channel just created");
    progress.dir_enqueued(1);

    // Dispatcher: for every work item, acquire a semaphore permit and spawn a
    // detached task. The permit is released when the task drops its guard, so
    // exactly `concurrency` tasks run at any moment across BOTH listings and
    // files. Root-level tasks push more work into `tx` before finishing.
    //
    // Termination: the main task blocks on `done_notify` until pending==0,
    // then drops the root tx AND signals the dispatcher via `stop_dispatch`.
    // The dispatcher checks that signal each iteration (via `try_recv`) and
    // exits, dropping its own tx. In-flight workers eventually finish and
    // drop their tx clones. That's the last sender, so rx.recv() would return
    // None even if the dispatcher weren't already gone — belt AND braces.
    let stop_dispatch = Arc::new(tokio::sync::Notify::new());
    let dispatcher = {
        let dtx = tx.clone();
        let sem = sem.clone();
        let cfg = cfg.clone();
        let client = client.clone();
        let manifest = manifest.clone();
        let matcher = matcher.clone();
        let visited = visited.clone();
        let stats = stats.clone();
        let pending = pending.clone();
        let done_notify = done_notify.clone();
        let progress_disp = progress.clone();
        let downloaded_since_ckpt = downloaded_since_ckpt.clone();
        let manifest_path = manifest_path.clone();
        let stop_dispatch = stop_dispatch.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stop_dispatch.notified() => break,
                    maybe = rx.recv() => {
                        let Some(work) = maybe else { break };
                        let permit = sem
                            .clone()
                            .acquire_owned()
                            .await
                            .expect("semaphore never closed");
                        let tx = dtx.clone();
                        let cfg = cfg.clone();
                        let client = client.clone();
                        let manifest = manifest.clone();
                        let matcher = matcher.clone();
                        let visited = visited.clone();
                        let stats = stats.clone();
                        let pending = pending.clone();
                        let done_notify = done_notify.clone();
                        let progress = progress_disp.clone();
                        let downloaded_since_ckpt = downloaded_since_ckpt.clone();
                        let manifest_path = manifest_path.clone();
                        tokio::spawn(async move {
                            match work {
                                Work::Listing { url, depth } => {
                                    handle_listing(
                                        &url, depth, &cfg, &client, &matcher, &visited, &stats,
                                        &tx, &pending, &progress, retry,
                                    )
                                    .await;
                                }
                                Work::File(task) => {
                                    handle_file(
                                        &task, &cfg, &client, &manifest, &stats, &progress,
                                        &downloaded_since_ckpt, &manifest_path, retry,
                                    )
                                    .await;
                                }
                            }
                            if pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                                done_notify.notify_one();
                            }
                            drop(permit);
                        });
                    }
                }
            }
        })
    };

    // Wait until every enqueued item (root + everything it discovers,
    // recursively) has finished. Then signal the dispatcher to stop and drop
    // the root sender.
    done_notify.notified().await;
    stop_dispatch.notify_one();
    drop(tx);
    let _ = dispatcher.await;

    // Final flush.
    if !config.dry_run {
        manifest
            .checkpoint()
            .await
            .with_context(|| format!("failed to write manifest {}", manifest_path.display()))?;
    }

    stop.abort();
    // Unwrap the stats Mutex — no other holders remain after the dispatcher
    // task joined above.
    let stats_final = Arc::try_unwrap(stats)
        .map(Mutex::into_inner)
        .unwrap_or_else(|arc| arc.blocking_lock().clone());
    stats_final.summarize();
    Ok(stats_final.final_status())
}

/// Fetch a directory listing, mark it visited, parse its entries, and enqueue
/// each entry as more work (subdir or file download). Increments `pending`
/// exactly once per enqueue so the termination counter stays honest.
async fn handle_listing(
    url: &Url,
    depth: u32,
    config: &AppConfig,
    client: &Client,
    matcher: &Matcher,
    visited: &Mutex<HashSet<String>>,
    stats: &Mutex<Stats>,
    tx: &mpsc::UnboundedSender<Work>,
    pending: &AtomicUsize,
    progress: &crate::progress::Progress,
    retry: RetryConfig,
) {
    // Dedup: two listings that resolve to the same normalized URL count as
    // one. We hold the lock only across the insert to keep the critical
    // section tiny.
    let normalized = normalize_url(url);
    {
        let mut v = visited.lock().await;
        if !v.insert(normalized) {
            progress.dir_completed();
            return;
        }
    }

    let Some(relative_dir) = relative_path(&config.root_url, url) else {
        log_info(
            config.log_level,
            &format!("Skipping outside-scope URL: {url}"),
        );
        progress.dir_completed();
        return;
    };

    progress.task_started();
    let listing = fetcher::fetch_listing(client, url, retry, config.log_level).await;
    progress.listing_finished();

    match listing {
        ListingFetch::Body(body) => {
            if !config.dry_run {
                let local_dir = config.output.join(&relative_dir);
                if let Err(err) = fs::create_dir_all(&local_dir).await {
                    log_info(
                        config.log_level,
                        &format!("failed to create directory {}: {err}", local_dir.display()),
                    );
                    stats.lock().await.files_failed += 1;
                    progress.dir_completed();
                    return;
                }
            }
            stats.lock().await.dirs_processed += 1;

            let mut entries = parse_listing_entries(&body, url);
            entries.sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));

            for entry in entries {
                if !is_under_root(&config.root_url, &entry.url) {
                    continue;
                }
                let Some(rel_path) = relative_path(&config.root_url, &entry.url) else {
                    continue;
                };
                if entry.is_dir {
                    // Depth cap: skip subdirs that would exceed the user's
                    // `--depth` budget. `depth` here is the current listing's
                    // depth; the subdir would be `depth + 1`. `None` means
                    // unlimited, so the check reduces to "always false".
                    if let Some(max) = config.depth
                        && depth + 1 > max
                    {
                        log_debug(
                            config.log_level,
                            &format!(
                                "Depth cap ({max}) reached, not descending into {}",
                                entry.url
                            ),
                        );
                        continue;
                    }
                    let sub_url = ensure_trailing_slash(entry.url);
                    pending.fetch_add(1, Ordering::AcqRel);
                    progress.dir_enqueued(1);
                    // Send can only fail if the receiver was dropped, which
                    // only happens after the whole crawl finishes — impossible
                    // here because we hold at least our own pending count.
                    if tx
                        .send(Work::Listing {
                            url: sub_url,
                            depth: depth + 1,
                        })
                        .is_err()
                    {
                        pending.fetch_sub(1, Ordering::AcqRel);
                        progress.dir_completed();
                    }
                    continue;
                }
                if !matcher.is_allowed(&rel_path) {
                    stats.lock().await.files_skipped += 1;
                    log_debug(
                        config.log_level,
                        &format!("Filtered out {}", rel_path.to_string_lossy()),
                    );
                    continue;
                }
                pending.fetch_add(1, Ordering::AcqRel);
                if tx
                    .send(Work::File(DownloadTask {
                        file_url: entry.url,
                        relative_path: rel_path,
                        source: EntrySource::ListingFile,
                    }))
                    .is_err()
                {
                    pending.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
        ListingFetch::Skipped => stats.lock().await.files_skipped += 1,
        ListingFetch::Failed => stats.lock().await.files_failed += 1,
    }
    progress.dir_completed();
}

/// Fetch one file and record its outcome. Runs the mid-download checkpoint
/// side-effect so a crash after N downloaded files leaves a resumable manifest.
async fn handle_file(
    task: &DownloadTask,
    config: &AppConfig,
    client: &Client,
    manifest: &ManifestStore,
    stats: &Mutex<Stats>,
    progress: &crate::progress::Progress,
    downloaded_since_ckpt: &AtomicUsize,
    manifest_path: &Path,
    retry: RetryConfig,
) {
    progress.task_started();
    let outcome = process_single_task(client, config, manifest, retry, task.clone()).await;
    match &outcome {
        TaskOutcome::Downloaded { url, entry } => {
            // Bytes already accounted for via bytes_delta during the stream;
            // file_completed_streamed just bumps the file count.
            progress.file_completed_streamed();
            manifest.record(normalize_url(url), entry.clone()).await;
            let mut s = stats.lock().await;
            s.files_downloaded += 1;
            drop(s);

            // Periodic checkpoint. A single worker "wins" the checkpoint when
            // its increment crosses the interval boundary; the others just
            // fall through with a cheap compare-swap loop.
            let prev = downloaded_since_ckpt.fetch_add(1, Ordering::AcqRel);
            if !config.dry_run && (prev + 1) % CHECKPOINT_INTERVAL == 0 {
                if let Err(err) = manifest.checkpoint().await {
                    log_info(
                        config.log_level,
                        &format!(
                            "Manifest checkpoint failed at {}: {err:#}",
                            manifest_path.display()
                        ),
                    );
                    stats.lock().await.warnings += 1;
                }
            }
        }
        TaskOutcome::Skipped => {
            progress.file_skipped();
            stats.lock().await.files_skipped += 1;
        }
        TaskOutcome::Failed => {
            progress.file_failed();
            stats.lock().await.files_failed += 1;
        }
    }
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

    // Live-throughput hook: bump the global bytes counter as chunks arrive so
    // the spinner reflects real-time progress (particularly useful on big
    // files). We could throttle these to every N KB, but AtomicU64::fetch_add
    // is cheap enough that per-chunk updates aren't a hotspot in practice.
    let progress = crate::progress::active();
    let progress_for_cb = progress.clone();
    let on_chunk = move |delta: u64| progress_for_cb.bytes_delta(delta);

    let dest = if config.dry_run {
        None
    } else {
        Some(output_path.as_path())
    };

    let fetched = fetcher::fetch_file(
        client,
        &task.file_url,
        task.source,
        &resume,
        retry,
        config.log_level,
        dest,
        Some(&on_chunk),
    )
    .await;
    match fetched {
        FileFetch::Downloaded(payload) => {
            if config.dry_run {
                log_info(
                    config.log_level,
                    &format!("[dry-run] Would download {}", task.file_url),
                );
                return TaskOutcome::Skipped;
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
