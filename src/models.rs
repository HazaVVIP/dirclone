use crate::errors::FinalStatus;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use url::Url;

/// Where a listing entry came from. A `ListingFile` is a path the parent
/// listing advertised as a file — even if the server later serves it as
/// text/html, we must save it (it's a real file the operator wants cloned).
/// A `DirCandidate` is a URL we intend to crawl as a directory; an html body
/// there is the listing itself, not a file to save.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntrySource {
    ListingFile,
    DirCandidate,
}

#[derive(Debug, Clone)]
pub struct ListingEntry {
    pub url: Url,
    pub is_dir: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub local_path: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub files: HashMap<String, ManifestEntry>,
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub dirs_processed: usize,
    pub files_downloaded: usize,
    pub files_skipped: usize,
    pub files_failed: usize,
    pub warnings: usize,
}

impl Stats {
    pub fn summarize(&self) {
        crate::progress::println(&format!(
            "Summary: dirs={}, downloaded={}, skipped={}, failed={}, warnings={}",
            self.dirs_processed,
            self.files_downloaded,
            self.files_skipped,
            self.files_failed,
            self.warnings
        ));
    }

    pub fn final_status(&self) -> FinalStatus {
        if self.files_failed > 0 {
            FinalStatus::PartialFailure
        } else {
            FinalStatus::Success
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub file_url: Url,
    pub relative_path: PathBuf,
    pub source: EntrySource,
}
