use dirclone::cli::{AppConfig, LogLevel};
use dirclone::crawler;
use dirclone::errors::FinalStatus;
use mockito::Server;
use tempfile::tempdir;
use url::Url;

fn base_config(root_url: Url, output: std::path::PathBuf) -> AppConfig {
    AppConfig {
        root_url,
        output,
        timeout_seconds: 10,
        read_timeout: 30,
        connect_timeout: 5,
        user_agent: "dirclone-test".to_string(),
        retries: 1,
        retry_backoff_ms: 1,
        max_redirects: 5,
        includes: vec![],
        excludes: vec![],
        dry_run: false,
        concurrency: 2,
        depth: None,
        force: false,
        manifest: ".manifest.json".into(),
        log_level: LogLevel::Quiet,
        no_progress: true,
    }
}

#[tokio::test]
async fn clones_recursive_and_skips_restricted() {
    let mut server = Server::new_async().await;
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\nsub/\nrestricted/\n")
        .create_async()
        .await;
    let _a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("hello")
        .create_async()
        .await;
    let _sub = server
        .mock("GET", "/root/sub/")
        .with_status(200)
        .with_body("<a href=\"b.txt\">b.txt</a>")
        .create_async()
        .await;
    let _b = server
        .mock("GET", "/root/sub/b.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("world")
        .create_async()
        .await;
    let _restricted = server
        .mock("GET", "/root/restricted/")
        .with_status(403)
        .create_async()
        .await;

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let config = base_config(root_url, tmp.path().to_path_buf());

    let status = crawler::run(&config).await.unwrap();
    assert_eq!(status, FinalStatus::Success);
    assert!(tmp.path().join("a.txt").exists());
    assert!(tmp.path().join("sub/b.txt").exists());
    assert!(!tmp.path().join("restricted").exists());
}

#[tokio::test]
async fn dry_run_does_not_write_files() {
    let mut server = Server::new_async().await;
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\n")
        .create_async()
        .await;
    let _a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("hello")
        .create_async()
        .await;

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let mut config = base_config(root_url, tmp.path().to_path_buf());
    config.dry_run = true;

    let status = crawler::run(&config).await.unwrap();
    assert_eq!(status, FinalStatus::Success);
    assert!(!tmp.path().join("a.txt").exists());
}

#[tokio::test]
async fn resume_manifest_skips_second_download() {
    let mut server = Server::new_async().await;
    let root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\n")
        .expect(2)
        .create_async()
        .await;
    let a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("hello")
        .expect(1)
        .create_async()
        .await;

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let config = base_config(root_url, tmp.path().to_path_buf());

    assert_eq!(crawler::run(&config).await.unwrap(), FinalStatus::Success);
    assert_eq!(crawler::run(&config).await.unwrap(), FinalStatus::Success);

    root.assert();
    a.assert();
}

/// Regression test for defect #1: directory traversal must be concurrent, not
/// serial. We assert this deterministically with a high-water-mark counter:
/// each directory handler bumps a shared counter, sleeps briefly, then lowers it,
/// recording the max seen. If traversal were serial, the max would be 1.
#[tokio::test]
async fn directory_traversal_is_concurrent() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let in_flight = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicUsize::new(0));

    let make_handler = |in_flight: Arc<AtomicUsize>, high_water: Arc<AtomicUsize>| {
        move |w: &mut dyn std::io::Write| -> std::io::Result<()> {
            let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            high_water.fetch_max(cur, Ordering::SeqCst);
            // mockito runs body writers on a blocking thread; a short sleep is
            // enough to force overlap when concurrency >= 2.
            std::thread::sleep(std::time::Duration::from_millis(120));
            in_flight.fetch_sub(1, Ordering::SeqCst);
            w.write_all(&[])?;
            Ok(())
        }
    };

    let mut server = Server::new_async().await;
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("left/\nright/\n")
        .create_async()
        .await;
    server
        .mock("GET", "/root/left/")
        .with_status(200)
        .with_chunked_body(make_handler(in_flight.clone(), high_water.clone()))
        .create_async()
        .await;
    server
        .mock("GET", "/root/right/")
        .with_status(200)
        .with_chunked_body(make_handler(in_flight, high_water.clone()))
        .create_async()
        .await;

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let mut config = base_config(root_url, tmp.path().to_path_buf());
    config.concurrency = 4;

    crawler::run(&config).await.unwrap();

    assert!(
        high_water.load(Ordering::SeqCst) >= 2,
        "expected concurrent directory fetches, max in-flight was {}",
        high_water.load(Ordering::SeqCst)
    );
}

/// Defect #3: a second run against a server that answers 304 Not Modified must
/// not re-download the body. The server should see exactly one body fetch.
#[tokio::test]
async fn conditional_get_skips_unchanged_file() {
    let mut server = Server::new_async().await;
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\n")
        .create_async()
        .await;
    // The file is fetched exactly once across both runs: second run gets a 304.
    let a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_header("etag", "\"v1\"")
        .with_body("hello")
        .expect(1)
        .create_async()
        .await;

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let config = base_config(root_url, tmp.path().to_path_buf());

    // First run: downloads, records etag.
    assert_eq!(crawler::run(&config).await.unwrap(), FinalStatus::Success);
    assert!(tmp.path().join("a.txt").exists());

    // Replace the mock with a 304 responder for the second run. Mockito matches
    // later-created mocks first, so this shadows the 200 without re-fetching body.
    let _not_modified = server
        .mock("GET", "/root/a.txt")
        .with_status(304)
        .with_header("etag", "\"v1\"")
        .create_async()
        .await;

    assert_eq!(crawler::run(&config).await.unwrap(), FinalStatus::Success);
    // File still present and unchanged.
    let content = std::fs::read_to_string(tmp.path().join("a.txt")).unwrap();
    assert_eq!(content, "hello");

    a.assert();
}

/// `--depth N` stops recursion at N levels below the root. With `depth=1`,
/// the root listing and its direct children fetch; grandchildren must not.
#[tokio::test]
async fn depth_cap_blocks_deeper_directories() {
    let mut server = Server::new_async().await;
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\nsub/\n")
        .create_async()
        .await;
    let _a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_body("hi")
        .create_async()
        .await;
    let _sub = server
        .mock("GET", "/root/sub/")
        .with_status(200)
        .with_body("b.txt\ndeep/\n")
        .create_async()
        .await;
    let _b = server
        .mock("GET", "/root/sub/b.txt")
        .with_status(200)
        .with_body("hey")
        .create_async()
        .await;
    // If dirclone descends into /root/sub/deep/ despite --depth=1, mockito
    // records a hit here. We ensure it does NOT by making the mock's
    // expected() equal 0 and calling .assert() at the end.
    let deep = server
        .mock("GET", "/root/sub/deep/")
        .with_status(200)
        .with_body("")
        .expect(0)
        .create_async()
        .await;

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let mut config = base_config(root_url, tmp.path().to_path_buf());
    config.depth = Some(1);

    let status = crawler::run(&config).await.unwrap();
    assert_eq!(status, FinalStatus::Success);
    assert!(tmp.path().join("a.txt").exists());
    assert!(tmp.path().join("sub/b.txt").exists());
    // Grandchild directory must not have been fetched, so no local dir either.
    assert!(!tmp.path().join("sub/deep").exists());
    deep.assert();
}

/// `--depth 0` means "only files at the root listing" — no recursion at all.
#[tokio::test]
async fn depth_zero_downloads_only_root_files() {
    let mut server = Server::new_async().await;
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\nsub/\n")
        .create_async()
        .await;
    let _a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_body("hi")
        .create_async()
        .await;
    let sub = server
        .mock("GET", "/root/sub/")
        .with_status(200)
        .with_body("")
        .expect(0)
        .create_async()
        .await;

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let mut config = base_config(root_url, tmp.path().to_path_buf());
    config.depth = Some(0);

    crawler::run(&config).await.unwrap();
    assert!(tmp.path().join("a.txt").exists());
    assert!(!tmp.path().join("sub").exists());
    sub.assert();
}

/// Defect #2: the manifest must be crash-safe (atomic, reloadable) so a killed
/// run can resume. We exercise the ManifestStore contract directly: after a
/// checkpoint, a fresh load sees the recorded entry.
#[tokio::test]
async fn manifest_checkpoint_is_persistent_and_reloadable() {
    use dirclone::manifest::ManifestStore;
    use dirclone::models::ManifestEntry;

    let tmp = tempdir().unwrap();
    let manifest_path = tmp.path().join(".manifest.json");

    let store = ManifestStore::load(&manifest_path).await.unwrap();
    store
        .record(
            "http://example/root/a.txt".to_string(),
            ManifestEntry {
                local_path: "a.txt".to_string(),
                size: 5,
                etag: Some("\"v1\"".to_string()),
                last_modified: None,
            },
        )
        .await;
    // Before checkpoint: nothing on disk.
    assert!(!manifest_path.exists());

    store.checkpoint().await.unwrap();
    assert!(manifest_path.exists());

    // Reload and verify the entry survived.
    let reloaded = ManifestStore::load(&manifest_path).await.unwrap();
    let m = reloaded.lock().await;
    let entry = m
        .files
        .get("http://example/root/a.txt")
        .expect("entry lost");
    assert_eq!(entry.size, 5);
    assert_eq!(entry.etag.as_deref(), Some("\"v1\""));

    // Idempotent: a checkpoint with no new writes is a no-op (still valid JSON).
    drop(m);
    reloaded.checkpoint().await.unwrap();
    serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(&manifest_path).unwrap())
        .unwrap();
}
