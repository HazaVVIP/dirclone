use crate::cli::{LogLevel, log_debug, log_info};
use anyhow::{Result, anyhow};
use reqwest::StatusCode;
use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER};
use reqwest::{Client, Response};
use std::time::Duration;
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
    Downloaded(FilePayload),
    NotModified,
    Skipped,
    Failed,
}

#[derive(Debug)]
pub struct FilePayload {
    pub bytes: Vec<u8>,
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
    resume: &ResumeHints,
    retry: RetryConfig,
    log_level: LogLevel,
) -> FileFetch {
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

    response_to_file_payload(response, url, log_level).await
}

async fn response_to_file_payload(response: Response, url: &Url, log_level: LogLevel) -> FileFetch {
    let etag = header_string(response.headers(), &ETAG);
    let last_modified = header_string(response.headers(), &LAST_MODIFIED);
    let content_type = header_string(response.headers(), &reqwest::header::CONTENT_TYPE);

    // A text/html body on a *file* URL is suspicious: many misconfigured servers
    // serve a generated listing or an error page for a path that the parent
    // listing advertised as a file. The M3 fix tags entries with their source so
    // we can tell "this was a real file" from "this might be a nested listing";
    // for now we skip to preserve the prior conservative behavior.
    // ponytail: ceiling = save-as-file when EntrySource::ListingFile is wired (M3).
    if let Some(ct) = content_type.as_deref()
        && (ct.starts_with("text/html") || ct.starts_with("application/xhtml+xml"))
    {
        log_info(
            log_level,
            &format!("Skipping possible HTML listing body for file {url} ({ct})"),
        );
        return FileFetch::Skipped;
    }

    match response.bytes().await {
        Ok(body) => FileFetch::Downloaded(FilePayload {
            size: body.len() as u64,
            bytes: body.to_vec(),
            etag,
            last_modified,
            content_type,
        }),
        Err(_) => FileFetch::Failed,
    }
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
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
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
