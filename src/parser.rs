use crate::models::ListingEntry;
use scraper::{Html, Selector};
use std::collections::HashSet;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListingFormat {
    Html,
    PlainText,
    Unknown,
}

pub fn parse_listing_entries(body: &str, current_url: &Url) -> Vec<ListingEntry> {
    let format = detect_format(body);
    let mut entries = Vec::new();
    let mut dedupe = HashSet::new();

    match format {
        ListingFormat::Html => {
            // HTML body: only harvest <a href="..."> anchors. If the HTML has
            // ZERO anchors it isn't a directory listing at all — it's a file
            // that the server chose to serve as text/html (e.g. an SPA index
            // that Python SimpleHTTP will hand out when you GET a directory
            // that happens to contain index.html). Falling through to the
            // plain-text scan would then treat every attribute-looking token
            // ('name="viewport"', 'content="width=device-width,') as a href
            // and drown the crawler in 404s. Returning empty tells the caller
            // "no children here" — cheap and correct.
            parse_html_entries(body, current_url, &mut entries, &mut dedupe);
        }
        ListingFormat::PlainText => {
            parse_plain_text_entries(body, current_url, &mut entries, &mut dedupe);
        }
        ListingFormat::Unknown => {
            // No HTML markers and no plain-text lines — probably an empty
            // response or one whose format we can't classify. Try both
            // parsers cheaply; either produces zero entries on gibberish.
            parse_html_entries(body, current_url, &mut entries, &mut dedupe);
            if entries.is_empty() {
                parse_plain_text_entries(body, current_url, &mut entries, &mut dedupe);
            }
        }
    }

    entries
}

fn detect_format(body: &str) -> ListingFormat {
    let lower = body.to_ascii_lowercase();
    if lower.contains("<html") || lower.contains("<a ") || lower.contains("<pre") {
        ListingFormat::Html
    } else if body.lines().any(|line| !line.trim().is_empty()) {
        ListingFormat::PlainText
    } else {
        ListingFormat::Unknown
    }
}

fn parse_html_entries(
    body: &str,
    current_url: &Url,
    entries: &mut Vec<ListingEntry>,
    dedupe: &mut HashSet<String>,
) {
    let Ok(anchor_selector) = Selector::parse("a") else {
        return;
    };

    let document = Html::parse_document(body);
    for anchor in document.select(&anchor_selector) {
        let Some(href) = anchor.value().attr("href") else {
            continue;
        };

        let href = href.trim();
        if href.is_empty() || href.starts_with('#') || href.starts_with('?') || href == "../" {
            continue;
        }

        let Ok(mut resolved) = current_url.join(href) else {
            continue;
        };
        resolved.set_fragment(None);

        let label = anchor
            .text()
            .collect::<Vec<_>>()
            .join("")
            .trim()
            .to_string();
        let is_dir = href.ends_with('/') || label.ends_with('/') || resolved.path().ends_with('/');

        push_unique_entry(entries, dedupe, resolved, is_dir);
    }
}

fn parse_plain_text_entries(
    body: &str,
    current_url: &Url,
    entries: &mut Vec<ListingEntry>,
    dedupe: &mut HashSet<String>,
) {
    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("Directory listing for") || line.contains('<') {
            continue;
        }

        let token = line.split_whitespace().next().unwrap_or_default();
        if token.is_empty() || token == "../" || token == "./" {
            continue;
        }

        let Ok(mut resolved) = current_url.join(token) else {
            continue;
        };
        resolved.set_fragment(None);

        let is_dir = token.ends_with('/');
        push_unique_entry(entries, dedupe, resolved, is_dir);
    }
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

fn normalize_url(url: &Url) -> String {
    let mut clone = url.clone();
    clone.set_fragment(None);
    clone.set_query(None);
    clone.to_string()
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
    fn dedupe_html_and_text_tokens() {
        let current = Url::parse("http://example.com/root/").unwrap();
        let body = r#"
            <a href="file.txt">file.txt</a>
            file.txt
        "#;

        let entries = parse_listing_entries(body, &current);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].url.as_str(), "http://example.com/root/file.txt");
    }

    /// Regression test: an SPA index served under a "directory-looking" URL
    /// (e.g. Python SimpleHTTP finds index.html when you GET foo/) MUST NOT
    /// be misparsed. Previously the HTML pass found zero anchors, so the
    /// plain-text fallback ate meta-tag attributes like `name="viewport"`
    /// and `content="width=device-width,` as filenames, producing garbled
    /// 404 URLs.
    #[test]
    fn html_with_no_anchors_yields_no_entries() {
        let current = Url::parse("http://example.com/nhda_app/").unwrap();
        let body = r#"<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta
      name="viewport"
      content="width=device-width, user-scalable=no, initial-scale=1.0"
    />
    <title></title>
  </head>
  <body>
    <div id="app"></div>
    <script type="module" src="/main.js"></script>
  </body>
</html>"#;

        let entries = parse_listing_entries(body, &current);
        assert!(
            entries.is_empty(),
            "SPA index must yield zero entries, got: {:?}",
            entries
                .iter()
                .map(|e| e.url.as_str())
                .collect::<Vec<_>>()
        );
    }
}
