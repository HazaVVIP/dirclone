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
        user_agent: "dirclone-test".to_string(),
        retries: 1,
        retry_backoff_ms: 1,
        max_redirects: 5,
        includes: vec![],
        excludes: vec![],
        dry_run: false,
        concurrency: 2,
        force: false,
        manifest: ".manifest.json".into(),
        log_level: LogLevel::Quiet,
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
