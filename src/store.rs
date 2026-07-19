use anyhow::{Context, Result};
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Write `bytes` to `path` atomically: stream to a sibling temp file, then
/// rename over the target. Avoids buffering whole files and avoids leaving a
/// half-written file on crash.
pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let Some(parent) = path.parent() else {
        anyhow::bail!("output path has no parent: {}", path.display());
    };
    fs::create_dir_all(parent)
        .await
        .with_context(|| format!("failed to create parent dir {}", parent.display()))?;

    let tmp = parent.join(format!(
        ".dirclone-tmp-{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("part")
    ));

    let mut file = fs::File::create(&tmp)
        .await
        .with_context(|| format!("failed to create temp file {}", tmp.display()))?;
    file.write_all(bytes)
        .await
        .with_context(|| format!("failed to write temp file {}", tmp.display()))?;
    file.flush()
        .await
        .with_context(|| format!("failed to flush temp file {}", tmp.display()))?;
    drop(file);

    fs::rename(&tmp, path)
        .await
        .with_context(|| format!("failed to move temp file into place {}", path.display()))?;
    Ok(())
}
