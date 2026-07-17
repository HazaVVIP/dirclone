use crate::cli::{AppConfig, LogLevel};
use crate::downloader::{self, FileFetch, ListingFetch, RetryConfig};
use crate::errors::FinalStatus;
use crate::models::{DownloadTask, Manifest, ManifestEntry, Stats};
use crate::parser::parse_listing_entries;
use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;
use reqwest::blocking::Client;
use reqwest::redirect::Policy;
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use url::Url;

pub fn run(config: &AppConfig) -> Result<FinalStatus> {
    if !config.dry_run {
        fs::create_dir_all(&config.output).with_context(|| {
            format!(
                "failed to create output directory {}",
                config.output.display()
            )
        })?;
    }

    let manifest_path = resolve_manifest_path(config);
    let mut manifest = load_manifest(&manifest_path)?;

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

    let pool = build_pool(config.concurrency)?;
    let mut visited = HashSet::new();
    let mut queue = VecDeque::from([config.root_url.clone()]);
    let mut stats = Stats::default();

    while let Some(current_url) = queue.pop_front() {
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

        match downloader::fetch_listing(&client, &current_url, retry, config.log_level) {
            ListingFetch::Body(body) => {
                if !config.dry_run {
                    let local_dir = config.output.join(&relative_dir);
                    fs::create_dir_all(&local_dir).with_context(|| {
                        format!("failed to create directory {}", local_dir.display())
                    })?;
                }
                stats.dirs_processed += 1;
                let mut entries = parse_listing_entries(&body, &current_url);
                entries.sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));

                let mut file_tasks = Vec::new();
                for entry in entries {
                    if !is_under_root(&config.root_url, &entry.url) {
                        continue;
                    }

                    let Some(rel_path) = relative_path(&config.root_url, &entry.url) else {
                        continue;
                    };

                    if entry.is_dir {
                        queue.push_back(ensure_trailing_slash(entry.url));
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

                let outcomes = process_tasks(&client, config, &manifest, retry, &pool, file_tasks);
                apply_outcomes(&mut manifest, &mut stats, outcomes);
            }
            ListingFetch::Skipped => stats.files_skipped += 1,
            ListingFetch::Failed => {
                stats.files_failed += 1;
            }
        }
    }

    if !config.dry_run {
        save_manifest(&manifest_path, &manifest)?;
    }

    stats.summarize();
    Ok(stats.final_status())
}

#[derive(Debug)]
enum TaskOutcome {
    Downloaded { url: Url, entry: ManifestEntry },
    Skipped,
    Failed,
}

fn process_tasks(
    client: &Client,
    config: &AppConfig,
    manifest: &Manifest,
    retry: RetryConfig,
    pool: &Option<ThreadPool>,
    tasks: Vec<DownloadTask>,
) -> Vec<TaskOutcome> {
    if tasks.is_empty() {
        return Vec::new();
    }

    let mut ordered_tasks = tasks;
    ordered_tasks.sort_by(|a, b| a.file_url.as_str().cmp(b.file_url.as_str()));

    let run = || {
        ordered_tasks
            .par_iter()
            .map(|task| process_single_task(client, config, manifest, retry, task))
            .collect::<Vec<_>>()
    };

    if let Some(pool) = pool {
        pool.install(run)
    } else {
        ordered_tasks
            .iter()
            .map(|task| process_single_task(client, config, manifest, retry, task))
            .collect()
    }
}

fn process_single_task(
    client: &Client,
    config: &AppConfig,
    manifest: &Manifest,
    retry: RetryConfig,
    task: &DownloadTask,
) -> TaskOutcome {
    let output_path = config.output.join(&task.relative_path);
    if is_resume_hit(config, manifest, &task.file_url, &output_path) {
        log_debug(
            config.log_level,
            &format!("Resume skip for {}", task.file_url),
        );
        return TaskOutcome::Skipped;
    }

    let fetched = downloader::fetch_file(client, &task.file_url, retry, config.log_level);
    let FileFetch::Downloaded(payload) = fetched else {
        return match fetched {
            FileFetch::Skipped => TaskOutcome::Skipped,
            FileFetch::Failed => TaskOutcome::Failed,
            FileFetch::Downloaded(_) => unreachable!(),
        };
    };

    if config.dry_run {
        log_info(
            config.log_level,
            &format!("[dry-run] Would download {}", task.file_url),
        );
        return TaskOutcome::Skipped;
    }

    if !config.force
        && output_path.exists()
        && output_path
            .metadata()
            .map(|meta| meta.len() == payload.size)
            .unwrap_or(false)
    {
        log_debug(
            config.log_level,
            &format!("Skipping unchanged file {}", output_path.display()),
        );
        return TaskOutcome::Skipped;
    }

    if let Some(parent) = output_path.parent()
        && let Err(err) = fs::create_dir_all(parent)
    {
        log_info(
            config.log_level,
            &format!("Failed to create parent dir {}: {err}", parent.display()),
        );
        return TaskOutcome::Failed;
    }

    if let Err(err) = fs::write(&output_path, payload.bytes) {
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

fn apply_outcomes(manifest: &mut Manifest, stats: &mut Stats, outcomes: Vec<TaskOutcome>) {
    for outcome in outcomes {
        match outcome {
            TaskOutcome::Downloaded { url, entry } => {
                manifest.files.insert(normalize_url(&url), entry);
                stats.files_downloaded += 1;
            }
            TaskOutcome::Skipped => stats.files_skipped += 1,
            TaskOutcome::Failed => stats.files_failed += 1,
        }
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

fn build_pool(concurrency: usize) -> Result<Option<ThreadPool>> {
    if concurrency <= 1 {
        return Ok(None);
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(concurrency)
        .build()
        .context("failed to build rayon thread pool")?;
    Ok(Some(pool))
}

fn is_resume_hit(config: &AppConfig, manifest: &Manifest, url: &Url, output_path: &Path) -> bool {
    if config.force {
        return false;
    }

    let Some(entry) = manifest.files.get(&normalize_url(url)) else {
        return false;
    };

    if !output_path.exists() {
        return false;
    }

    let same_path = entry.local_path
        == normalize_path_string(&relative_from_output(&config.output, output_path));
    let same_size = output_path
        .metadata()
        .map(|meta| meta.len() == entry.size)
        .unwrap_or(false);

    same_path && same_size
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

fn load_manifest(path: &Path) -> Result<Manifest> {
    if !path.exists() {
        return Ok(Manifest::default());
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest {}", path.display()))?;
    let parsed = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse manifest {}", path.display()))?;
    Ok(parsed)
}

fn save_manifest(path: &Path, manifest: &Manifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create manifest dir {}", parent.display()))?;
    }

    let data = serde_json::to_string_pretty(manifest).context("failed to serialize manifest")?;
    fs::write(path, data)
        .with_context(|| format!("failed to write manifest {}", path.display()))?;
    Ok(())
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

fn log_info(log_level: LogLevel, message: &str) {
    if log_level >= LogLevel::Info {
        eprintln!("{message}");
    }
}

fn log_debug(log_level: LogLevel, message: &str) {
    if log_level >= LogLevel::Debug {
        eprintln!("[debug] {message}");
    }
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
