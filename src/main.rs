use anyhow::{Context, Result, bail};
use clap::Parser;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use url::Url;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Clone recursive web directory listings to local disk"
)]
struct Cli {
    /// URL root directory listing (must end with /)
    url: String,

    /// Output directory on local machine
    output: PathBuf,

    /// Timeout in seconds for HTTP requests
    #[arg(long, default_value_t = 20)]
    timeout_seconds: u64,

    /// Custom User-Agent header
    #[arg(long, default_value = "dirclone/0.1")]
    user_agent: String,
}

#[derive(Debug, Clone)]
struct ListingEntry {
    url: Url,
    is_dir: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let root_url = Url::parse(&cli.url).context("failed to parse root URL")?;

    if !root_url.path().ends_with('/') {
        bail!("root URL must end with '/': {}", root_url);
    }

    fs::create_dir_all(&cli.output)
        .with_context(|| format!("failed to create output directory {}", cli.output.display()))?;

    let client = Client::builder()
        .timeout(Duration::from_secs(cli.timeout_seconds))
        .user_agent(cli.user_agent)
        .build()
        .context("failed to create HTTP client")?;

    clone_recursive(&client, &root_url, &cli.output)?;
    Ok(())
}

fn clone_recursive(client: &Client, root_url: &Url, output_root: &Path) -> Result<()> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::from([root_url.clone()]);

    while let Some(current_url) = queue.pop_front() {
        let normalized = normalize_url(&current_url);
        if !visited.insert(normalized) {
            continue;
        }

        let Some(relative_dir) = relative_path(root_url, &current_url) else {
            eprintln!("Skipping outside-scope URL: {current_url}");
            continue;
        };

        let local_dir = output_root.join(relative_dir);
        fs::create_dir_all(&local_dir)
            .with_context(|| format!("failed to create directory {}", local_dir.display()))?;

        let response = match client.get(current_url.clone()).send() {
            Ok(resp) => resp,
            Err(err) => {
                eprintln!("Failed to read listing {current_url}: {err}");
                continue;
            }
        };

        if is_access_denied(response.status()) {
            eprintln!(
                "Skipping restricted directory {current_url} ({})",
                response.status()
            );
            continue;
        }

        if !response.status().is_success() {
            eprintln!(
                "Skipping directory {current_url}: HTTP {}",
                response.status()
            );
            continue;
        }

        let body = match response.text() {
            Ok(content) => content,
            Err(err) => {
                eprintln!("Failed reading response body {current_url}: {err}");
                continue;
            }
        };

        for entry in parse_listing_entries(&body, &current_url) {
            if !is_under_root(root_url, &entry.url) {
                continue;
            }

            if entry.is_dir {
                queue.push_back(ensure_trailing_slash(entry.url));
            } else if let Err(err) = download_file(client, root_url, output_root, &entry.url) {
                eprintln!("Failed to download {}: {err}", entry.url);
            }
        }
    }

    Ok(())
}

fn download_file(
    client: &Client,
    root_url: &Url,
    output_root: &Path,
    file_url: &Url,
) -> Result<()> {
    let Some(relative_file) = relative_path(root_url, file_url) else {
        return Ok(());
    };

    let response = client
        .get(file_url.clone())
        .send()
        .with_context(|| format!("request failed for {}", file_url))?;

    if is_access_denied(response.status()) {
        eprintln!(
            "Skipping restricted file {} ({})",
            file_url,
            response.status()
        );
        return Ok(());
    }

    if !response.status().is_success() {
        eprintln!("Skipping file {}: HTTP {}", file_url, response.status());
        return Ok(());
    }

    let bytes = response
        .bytes()
        .with_context(|| format!("failed to read file body {}", file_url))?;

    let local_path = output_root.join(relative_file);
    if let Some(parent) = local_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent dir {}", parent.display()))?;
    }

    fs::write(&local_path, &bytes)
        .with_context(|| format!("failed to write file {}", local_path.display()))?;

    Ok(())
}

fn parse_listing_entries(body: &str, current_url: &Url) -> Vec<ListingEntry> {
    let mut entries = Vec::new();
    let mut dedupe = HashSet::new();

    if let Ok(anchor_selector) = Selector::parse("a") {
        let document = Html::parse_document(body);
        for anchor in document.select(&anchor_selector) {
            let Some(href) = anchor.value().attr("href") else {
                continue;
            };

            if href.is_empty() || href.starts_with('#') || href.starts_with('?') || href == "../" {
                continue;
            }

            let resolved = match current_url.join(href) {
                Ok(url) => url,
                Err(_) => continue,
            };

            let label: String = anchor
                .text()
                .collect::<Vec<_>>()
                .join("")
                .trim()
                .to_string();
            let is_dir =
                href.ends_with('/') || label.ends_with('/') || resolved.path().ends_with('/');
            push_unique_entry(&mut entries, &mut dedupe, resolved, is_dir);
        }
    }

    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("Directory listing for") || line.contains('<') {
            continue;
        }

        let token = line.split_whitespace().next().unwrap_or_default();
        if token.is_empty() || token == "../" || token == "./" {
            continue;
        }

        let resolved = match current_url.join(token) {
            Ok(url) => url,
            Err(_) => continue,
        };

        let is_dir = token.ends_with('/');
        push_unique_entry(&mut entries, &mut dedupe, resolved, is_dir);
    }

    entries
}

fn push_unique_entry(
    entries: &mut Vec<ListingEntry>,
    dedupe: &mut HashSet<String>,
    url: Url,
    is_dir: bool,
) {
    let normalized = normalize_url(&url);
    if dedupe.insert(normalized) {
        entries.push(ListingEntry { url, is_dir });
    }
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
    if cleaned.is_empty() {
        Some(PathBuf::new())
    } else {
        Some(PathBuf::from(cleaned))
    }
}

fn ensure_trailing_slash(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    url
}

fn normalize_url(url: &Url) -> String {
    let mut clone = url.clone();
    clone.set_fragment(None);
    clone.to_string()
}

fn is_access_denied(status: StatusCode) -> bool {
    status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_text_listing() {
        let current = Url::parse("http://example.com/.hermes/").unwrap();
        let body = "Directory listing for /.hermes/\n.env\nbin/\n";
        let entries = parse_listing_entries(body, &current);

        assert!(
            entries
                .iter()
                .any(|e| e.url.as_str() == "http://example.com/.hermes/.env" && !e.is_dir)
        );
        assert!(
            entries
                .iter()
                .any(|e| e.url.as_str() == "http://example.com/.hermes/bin/" && e.is_dir)
        );
    }

    #[test]
    fn parse_html_listing() {
        let current = Url::parse("http://example.com/root/").unwrap();
        let body = r#"
            <html><body>
              <a href="../">Parent</a>
              <a href="folder/">folder/</a>
              <a href="file.txt">file.txt</a>
            </body></html>
        "#;

        let entries = parse_listing_entries(body, &current);
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|e| e.url.as_str() == "http://example.com/root/folder/" && e.is_dir)
        );
        assert!(
            entries
                .iter()
                .any(|e| e.url.as_str() == "http://example.com/root/file.txt" && !e.is_dir)
        );
    }

    #[test]
    fn relative_path_returns_none_for_outside_scope() {
        let root = Url::parse("http://example.com/root/").unwrap();
        let other = Url::parse("http://example.com/another/file.txt").unwrap();
        assert!(relative_path(&root, &other).is_none());
    }
}
