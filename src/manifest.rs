use crate::models::Manifest;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;

/// Crash-safe manifest store. Writes are atomic (temp + rename) and can be
/// flushed on demand. The crawler checkpoints periodically and on SIGINT so a
/// killed run resumes from the last checkpoint instead of losing everything.
pub struct ManifestStore {
    inner: Arc<Mutex<Manifest>>,
    path: Option<PathBuf>,
    /// Records modified since the last flush, to decide whether a checkpoint
    /// is a no-op.
    dirty: Arc<Mutex<bool>>,
}

impl ManifestStore {
    pub async fn load(path: &Path) -> Result<Self> {
        let manifest = if path.exists() {
            let content = fs::read_to_string(path)
                .await
                .with_context(|| format!("failed to read manifest {}", path.display()))?;
            serde_json::from_str(&content)
                .with_context(|| format!("failed to parse manifest {}", path.display()))?
        } else {
            Manifest::default()
        };

        // Ensure the parent dir exists so the first checkpoint can write.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create manifest dir {}", parent.display()))?;
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(manifest)),
            path: Some(path.to_path_buf()),
            dirty: Arc::new(Mutex::new(false)),
        })
    }

    /// In-memory store for dry-run: no path, checkpoint is a no-op.
    pub fn for_memory() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Manifest::default())),
            path: None,
            dirty: Arc::new(Mutex::new(false)),
        }
    }

    /// True when this store never persists (dry-run mode).
    pub fn is_dry_run(&self) -> bool {
        self.path.is_none()
    }

    /// Acquire the manifest lock. Callers should keep the guard short and call
    /// `mark_dirty()` (or rely on `record`) before releasing if they mutated.
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, Manifest> {
        self.inner.lock().await
    }

    /// Record a downloaded file. Marks the store dirty for the next checkpoint.
    pub async fn record(&self, key: String, entry: crate::models::ManifestEntry) {
        let mut m = self.inner.lock().await;
        m.files.insert(key, entry);
        *self.dirty.lock().await = true;
    }

    /// Flush to disk if dirty. Atomic: write to temp, rename over the target.
    /// Safe to call concurrently with downloads; only the lock is held briefly.
    pub async fn checkpoint(&self) -> Result<()> {
        let dirty = *self.dirty.lock().await;
        if !dirty {
            return Ok(());
        }
        let m = self.inner.lock().await;
        let data = serde_json::to_string_pretty(&*m).context("failed to serialize manifest")?;
        drop(m);

        let Some(path) = &self.path else {
            return Ok(()); // in-memory store
        };
        write_atomic_string(path, &data).await?;
        *self.dirty.lock().await = false;
        Ok(())
    }
}

async fn write_atomic_string(path: &Path, data: &str) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, data)
        .await
        .with_context(|| format!("failed to write manifest temp {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .await
        .with_context(|| format!("failed to rename manifest into place {}", path.display()))?;
    Ok(())
}
