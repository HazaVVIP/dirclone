use crate::cli::LogLevel;
use anyhow::{Result, anyhow};
use reqwest::StatusCode;
use reqwest::blocking::{Client, Response};
use std::thread;
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
    Skipped,
    Failed,
}

#[derive(Debug)]
pub struct FilePayload {
    pub bytes: Vec<u8>,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub fn fetch_listing(
    client: &Client,
    url: &Url,
    retry: RetryConfig,
    log_level: LogLevel,
) -> ListingFetch {
    let response = match send_with_retry(client, url, retry, log_level) {
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

    match response.text() {
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

pub fn fetch_file(
    client: &Client,
    url: &Url,
    retry: RetryConfig,
    log_level: LogLevel,
) -> FileFetch {
    let response = match send_with_retry(client, url, retry, log_level) {
        Ok(resp) => resp,
        Err(err) => {
            log_info(log_level, &format!("Request failed for {url}: {err:#}"));
            return FileFetch::Failed;
        }
    };

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

    if let Some(content_type) = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase())
        && (content_type.starts_with("text/html")
            || content_type.starts_with("application/xhtml+xml"))
    {
        log_info(
            log_level,
            &format!("Skipping possible HTML listing body for file {url} ({content_type})"),
        );
        return FileFetch::Skipped;
    }

    response_to_file_payload(response)
}

fn response_to_file_payload(response: Response) -> FileFetch {
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let last_modified = response
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);

    match response.bytes() {
        Ok(body) => FileFetch::Downloaded(FilePayload {
            size: body.len() as u64,
            bytes: body.to_vec(),
            etag,
            last_modified,
        }),
        Err(_) => FileFetch::Failed,
    }
}

fn send_with_retry(
    client: &Client,
    url: &Url,
    retry: RetryConfig,
    log_level: LogLevel,
) -> Result<Response> {
    let mut wait_ms = retry.retry_backoff_ms;
    for attempt in 0..=retry.retries {
        let result = client.get(url.clone()).send();
        match result {
            Ok(resp) => {
                if should_retry_status(resp.status()) && attempt < retry.retries {
                    log_debug(
                        log_level,
                        &format!("Retrying {url} due to status {}", resp.status()),
                    );
                    sleep_backoff(wait_ms);
                    wait_ms = wait_ms.saturating_mul(2);
                    continue;
                }
                return Ok(resp);
            }
            Err(err) => {
                if is_retryable_request_error(&err) && attempt < retry.retries {
                    log_debug(log_level, &format!("Retrying {url} due to error: {err}"));
                    sleep_backoff(wait_ms);
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

fn sleep_backoff(wait_ms: u64) {
    if wait_ms > 0 {
        thread::sleep(Duration::from_millis(wait_ms));
    }
}

fn is_access_denied(status: StatusCode) -> bool {
    status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED
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
