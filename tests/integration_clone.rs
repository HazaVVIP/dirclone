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

#[test]
fn clones_recursive_and_skips_restricted() {
    let mut server = Server::new();
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\nsub/\nrestricted/\n")
        .create();
    let _a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("hello")
        .create();
    let _sub = server
        .mock("GET", "/root/sub/")
        .with_status(200)
        .with_body("<a href=\"b.txt\">b.txt</a>")
        .create();
    let _b = server
        .mock("GET", "/root/sub/b.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("world")
        .create();
    let _restricted = server
        .mock("GET", "/root/restricted/")
        .with_status(403)
        .create();

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let config = base_config(root_url, tmp.path().to_path_buf());

    let status = crawler::run(&config).unwrap();
    assert_eq!(status, FinalStatus::Success);
    assert!(tmp.path().join("a.txt").exists());
    assert!(tmp.path().join("sub/b.txt").exists());
    assert!(!tmp.path().join("restricted").exists());
}

#[test]
fn dry_run_does_not_write_files() {
    let mut server = Server::new();
    let _root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\n")
        .create();
    let _a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("hello")
        .create();

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let mut config = base_config(root_url, tmp.path().to_path_buf());
    config.dry_run = true;

    let status = crawler::run(&config).unwrap();
    assert_eq!(status, FinalStatus::Success);
    assert!(!tmp.path().join("a.txt").exists());
}

#[test]
fn resume_manifest_skips_second_download() {
    let mut server = Server::new();
    let root = server
        .mock("GET", "/root/")
        .with_status(200)
        .with_body("a.txt\n")
        .expect(2)
        .create();
    let a = server
        .mock("GET", "/root/a.txt")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("hello")
        .expect(1)
        .create();

    let tmp = tempdir().unwrap();
    let root_url = Url::parse(&format!("{}/root/", server.url())).unwrap();
    let config = base_config(root_url, tmp.path().to_path_buf());

    assert_eq!(crawler::run(&config).unwrap(), FinalStatus::Success);
    assert_eq!(crawler::run(&config).unwrap(), FinalStatus::Success);

    root.assert();
    a.assert();
}
