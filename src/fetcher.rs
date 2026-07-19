use crate::cli::{LogLevel, log_debug, log_info};
use crate::models::EntrySource;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, Response};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use url::Url;

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub retries: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Debug)]
pub enum ListingFetch {
    Body(String),
    Skipped,
    Failed,
}

#[derive(Debug)]
pub enum FileFetch {
    /// File was fully streamed to disk at `path`. Metadata only — no bytes in
    /// memory. When `dry_run` is set the caller passes `None` for `dest` and
    /// this variant reports `path = PathBuf::new()`.
    Downloaded(FilePayload),
    NotModified,
    Skipped,
    Failed,
}

#[derive(Debug)]
pub struct FilePayload {
    /// Final on-disk path (empty when dry_run skipped the write).
    pub path: PathBuf,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
}

/// Resume hints for a conditional GET. When present, the server may answer 304
/// and we treat the local file as already-current.
#[derive(Debug, Clone, Default)]
pub struct ResumeHints {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl ResumeHints {
    pub fn is_empty(&self) -> bool {
        self.etag.is_none() && self.last_modified.is_none()
    }
}

pub async fn fetch_listing(
    client: &Client,
    url: &Url,
    retry: RetryConfig,
    log_level: LogLevel,
) -> ListingFetch {
    let response = match send_with_retry(client, url, None, retry, log_level).await {
        Ok(resp) => resp,
        Err(err) => {
            log_info(log_level, &format!("Failed to read listing {url}: {err:#}"));
            return ListingFetch::Failed;
        }
    };

    if is_access_denied(response.status()) {
        log_info(
            log_level,
            &format!(
                "Skipping restricted directory {url} ({})",
                response.status()
            ),
        );
        return ListingFetch::Skipped;
    }

    if !response.status().is_success() {
        log_info(
            log_level,
            &format!("Skipping directory {url}: HTTP {}", response.status()),
        );
        return ListingFetch::Failed;
    }

    match response.text().await {
        Ok(body) => ListingFetch::Body(body),
        Err(err) => {
            log_info(
                log_level,
                &format!("Failed reading response body {url}: {err}"),
            );
            ListingFetch::Failed
        }
    }
}

pub async fn fetch_file(
    client: &Client,
    url: &Url,
    source: EntrySource,
    resume: &ResumeHints,
    retry: RetryConfig,
    log_level: LogLevel,
    // Destination. `None` = dry-run (headers/status only; response is dropped
    // without draining the body).
    dest: Option<&Path>,
    // Optional progress hook fired per streamed chunk. Receives cumulative
    // bytes-delta of THIS request (not global). Send+Sync so the fetch future
    // can be scheduled across worker threads.
    on_chunk: Option<&(dyn Fn(u64) + Send + Sync)>,
) -> FileFetch {
    // Mid-stream retry loop. `send_with_retry` handles connect/status failures
    // BEFORE the body arrives; a body that starts fine and then breaks (server
    // closes early, transient network hiccup) has to be retried at this outer
    // layer or the whole file is lost. Old HTTP/1.0 servers like Python's
    // SimpleHTTP were observed dropping ~5% of large-file connections mid-body.
    let max_stream_retries = retry.retries;
    let mut stream_attempt: u32 = 0;
    let mut backoff = retry.retry_backoff_ms.max(1);
    loop {
        let response = match send_with_retry(client, url, Some(resume), retry, log_level).await {
            Ok(resp) => resp,
            Err(err) => {
                log_info(log_level, &format!("Request failed for {url}: {err:#}"));
                return FileFetch::Failed;
            }
        };

        if response.status() == StatusCode::NOT_MODIFIED {
            log_debug(log_level, &format!("Not modified, keeping local {url}"));
            return FileFetch::NotModified;
        }

        if is_access_denied(response.status()) {
            log_info(
                log_level,
                &format!("Skipping restricted file {url} ({})", response.status()),
            );
            return FileFetch::Skipped;
        }

        if !response.status().is_success() {
            log_info(
                log_level,
                &format!("Skipping file {url}: HTTP {}", response.status()),
            );
            return FileFetch::Failed;
        }

        let outcome =
            stream_response_to_disk(response, url, source, log_level, dest, on_chunk).await;
        match outcome {
            FileFetch::Failed if stream_attempt < max_stream_retries => {
                stream_attempt += 1;
                log_debug(
                    log_level,
                    &format!(
                        "stream error for {url}; retry {stream_attempt}/{max_stream_retries}"
                    ),
                );
                sleep_backoff(backoff).await;
                // Cap the backoff so a series of stream failures doesn't wait
                // minutes between attempts. 8× the base is a reasonable ceiling.
                backoff = backoff
                    .saturating_mul(2)
                    .min(retry.retry_backoff_ms.saturating_mul(8).max(2000));
                continue;
            }
            FileFetch::Failed => {
                // Retries exhausted — surface at info level so the user knows
                // this file is genuinely gone. The mid-retry attempts stayed
                // silent (log_debug) to avoid flooding the terminal.
                log_info(
                    log_level,
                    &format!(
                        "Stream error for {url}: gave up after {} attempts",
                        max_stream_retries + 1
                    ),
                );
                return FileFetch::Failed;
            }
            other => return other,
        }
    }
}

/// Stream the response body chunk-by-chunk. On the happy path we write to a
/// sibling temp file and rename over the destination — same crash-safety as
/// the previous `write_atomic`, but overlapped with network I/O instead of
/// buffering the full body in RAM first.
async fn stream_response_to_disk(
    response: Response,
    url: &Url,
    source: EntrySource,
    log_level: LogLevel,
    dest: Option<&Path>,
    on_chunk: Option<&(dyn Fn(u64) + Send + Sync)>,
) -> FileFetch {
    let etag = header_string(response.headers(), &ETAG);
    let last_modified = header_string(response.headers(), &LAST_MODIFIED);
    let content_type = header_string(response.headers(), &reqwest::header::CONTENT_TYPE);

    // A text/html body on a URL the parent listing advertised as a *file* is a
    // real file (e.g. an index.html someone placed in the directory) — save it.
    // Only for a DirCandidate do we treat html as "probably a nested listing,
    // not the file we asked for" and skip. This fixes defect #4: previously a
    // legit index.html file sitting in a listing was silently dropped.
    if source == EntrySource::DirCandidate
        && let Some(ct) = content_type.as_deref()
        && (ct.starts_with("text/html") || ct.starts_with("application/xhtml+xml"))
    {
        log_info(
            log_level,
            &format!("Skipping possible HTML listing body for file {url} ({ct})"),
        );
        return FileFetch::Skipped;
    }

    // Dry-run: don't drain the body, just report the metadata. Dropping the
    // Response cancels the in-flight stream cleanly.
    let Some(dest) = dest else {
        return FileFetch::Downloaded(FilePayload {
            path: PathBuf::new(),
            size: 0,
            etag,
            last_modified,
            content_type,
        });
    };

    // Build the temp path in the destination's parent so the final rename is
    // guaranteed to be same-filesystem (rename across FSes fails on Linux).
    let Some(parent) = dest.parent() else {
        log_info(
            log_level,
            &format!("Refusing to stream {url}: destination has no parent"),
        );
        return FileFetch::Failed;
    };
    if let Err(err) = fs::create_dir_all(parent).await {
        log_info(
            log_level,
            &format!("Failed to create parent dir {}: {err}", parent.display()),
        );
        return FileFetch::Failed;
    }
    let tmp = parent.join(format!(
        ".dirclone-tmp-{}",
        dest.file_name().and_then(|n| n.to_str()).unwrap_or("part")
    ));

    let mut file = match fs::File::create(&tmp).await {
        Ok(f) => f,
        Err(err) => {
            log_info(
                log_level,
                &format!("Failed to create temp file {}: {err}", tmp.display()),
            );
            return FileFetch::Failed;
        }
    };

    let mut written: u64 = 0;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(b) => b,
            Err(err) => {
                // Downgraded from log_info to log_debug: the outer retry
                // loop in fetch_file logs the FINAL failure at info level
                // once all retries are exhausted. Every mid-retry attempt
                // being logged at info flooded the terminal on flaky
                // slow-server targets.
                log_debug(
                    log_level,
                    &format!("stream chunk error for {url} @ {written}B: {err}"),
                );
                let _ = fs::remove_file(&tmp).await;
                return FileFetch::Failed;
            }
        };
        if let Err(err) = file.write_all(&chunk).await {
            log_info(
                log_level,
                &format!("Write error for {}: {err}", tmp.display()),
            );
            let _ = fs::remove_file(&tmp).await;
            return FileFetch::Failed;
        }
        written = written.saturating_add(chunk.len() as u64);
        if let Some(cb) = on_chunk {
            cb(chunk.len() as u64);
        }
    }

    if let Err(err) = file.flush().await {
        log_info(
            log_level,
            &format!("Flush error for {}: {err}", tmp.display()),
        );
        let _ = fs::remove_file(&tmp).await;
        return FileFetch::Failed;
    }
    drop(file);

    if let Err(err) = fs::rename(&tmp, dest).await {
        log_info(
            log_level,
            &format!(
                "Failed to move {} into place at {}: {err}",
                tmp.display(),
                dest.display()
            ),
        );
        let _ = fs::remove_file(&tmp).await;
        return FileFetch::Failed;
    }

    FileFetch::Downloaded(FilePayload {
        path: dest.to_path_buf(),
        size: written,
        etag,
        last_modified,
        content_type,
    })
}

async fn send_with_retry(
    client: &Client,
    url: &Url,
    resume: Option<&ResumeHints>,
    retry: RetryConfig,
    log_level: LogLevel,
) -> Result<Response> {
    let mut wait_ms = retry.retry_backoff_ms.max(1);
    for attempt in 0..=retry.retries {
        let mut req = client.get(url.clone());
        if let Some(hints) = resume
            && !hints.is_empty()
        {
            if let Some(etag) = &hints.etag {
                req = req.header(IF_NONE_MATCH, etag);
            }
            if let Some(lm) = &hints.last_modified {
                req = req.header(IF_MODIFIED_SINCE, lm);
            }
        }
        match req.send().await {
            Ok(resp) => {
                if should_retry_status(resp.status()) && attempt < retry.retries {
                    let delay = retry_after_ms(&resp).unwrap_or(wait_ms);
                    log_debug(
                        log_level,
                        &format!("Retrying {url} due to status {}", resp.status()),
                    );
                    sleep_backoff(delay).await;
                    wait_ms = wait_ms.saturating_mul(2);
                    continue;
                }
                return Ok(resp);
            }
            Err(err) => {
                if is_retryable_request_error(&err) && attempt < retry.retries {
                    log_debug(log_level, &format!("Retrying {url} due to error: {err}"));
                    sleep_backoff(wait_ms).await;
                    wait_ms = wait_ms.saturating_mul(2);
                    continue;
                }
                return Err(anyhow!(err));
            }
        }
    }

    Err(anyhow!("unexpected retry loop termination for {url}"))
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn is_retryable_request_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

fn retry_after_ms(response: &Response) -> Option<u64> {
    let header = response.headers().get(RETRY_AFTER)?.to_str().ok()?;
    let trimmed = header.trim();
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Some(secs.saturating_mul(1000));
    }
    // Retry-After HTTP-date form is not common for open-directory servers; we
    // skip it rather than pull in a date parser. ponytail: ceiling = parse
    // RFC 7231 date if a target is ever observed to send it.
    None
}

async fn sleep_backoff(wait_ms: u64) {
    if wait_ms > 0 {
        // Full-jitter: pick a random delay uniformly in [0, wait_ms]. Prevents
        // a thundering herd of parallel workers from all retrying at the same
        // moment against a server that's already under stress. Source of
        // randomness is nanoseconds-since-UNIX-epoch mixed through a hash —
        // good enough for backoff spread; we don't need cryptographic
        // randomness here.
        use std::time::SystemTime;
        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        // Mix so back-to-back reads from adjacent workers (which see almost
        // identical clocks) produce well-separated jitter values.
        let mixed = now_ns
            .wrapping_mul(0x9E3779B97F4A7C15)
            .wrapping_add(0xBF58476D1CE4E5B9);
        let jitter = mixed % wait_ms.max(1);
        tokio::time::sleep(Duration::from_millis(jitter)).await;
    }
}

fn is_access_denied(status: StatusCode) -> bool {
    status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED
}

fn header_string(
    headers: &reqwest::header::HeaderMap,
    name: &reqwest::header::HeaderName,
) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}
