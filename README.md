# dirclone

Tools Rust untuk mereplikasi directory listing website ke local secara rekursif — cepat, resumable, dan sadar-hang.

## Fitur

- Clone file dan folder dari directory listing sampai kedalaman yang diinginkan (atau tanpa batas).
- Streaming download langsung ke disk (bukan buffer di RAM) — hemat memori dan siap file besar.
- Unified worker pool (mpsc + semaphore) — direktori dan file di-fetch paralel tanpa per-level barrier.
- HTTP compression aktif (`gzip`, `deflate`, `brotli`) + connection pooling + TCP keepalive.
- Parser berlapis untuk listing HTML (`<a href=...>`) dan plain text.
- Skip aman untuk `401`/`403`; retry + backoff untuk `5xx`/timeout; mid-stream retry untuk koneksi putus.
- Kontrol redirect (`--max-redirects`), depth (`--depth`), timeout (`--timeout-seconds` / `--connect-timeout`).
- Filter path dengan include/exclude glob.
- Proteksi path traversal saat mapping URL ke local path.
- Mode `--dry-run` untuk simulasi clone.
- Live progress bar dengan spinner anti-hang (auto-off jika stderr bukan TTY).
- Resume berbasis manifest lokal + conditional GET (ETag / Last-Modified) + mode `--force` untuk overwrite.
- Exit code:
  - `0` = sukses penuh
  - `2` = sukses parsial (ada file/listing gagal)
  - `1` = fatal error

## Install

Cara tercepat (Linux / macOS):

```bash
curl -fsSL https://raw.githubusercontent.com/HazaVVIP/dirclone/main/install.sh | bash
```

Skrip di atas mendeteksi OS/arch, mencoba pre-built binary dari GitHub Releases, dan otomatis fallback ke `cargo build --release` (memasang `rustup` jika perlu) lalu menempatkan biner di `/usr/local/bin/dirclone`.

Manual dari source:

```bash
git clone https://github.com/HazaVVIP/dirclone
cd dirclone
cargo build --release
install -m 755 target/release/dirclone ~/.cargo/bin/
```

## Usage

```bash
dirclone <URL> [OUTPUT_DIR] [OPTIONS]
```

`OUTPUT_DIR` opsional. Kalau tidak disebutkan, `dirclone` menurunkan namanya dari segmen path terakhir URL (fallback ke `<host>_<port>` jika URL menunjuk site root). Trailing `/` yang hilang pada URL di-append otomatis.

Contoh:

```bash
# Output otomatis → ./.hermes/
dirclone http://123.45.67.8:8080/.hermes

# Output eksplisit
dirclone http://123.45.67.8:8080/.hermes /tmp/hermes-clone

# URL di site root → ./123.45.67.8_8080/
dirclone http://123.45.67.8:8080/

# Batasi kedalaman: hanya root + 2 level subdir
dirclone http://123.45.67.8:8080/.hermes --depth 2

# Concurrency lebih agresif untuk target berbandwidth besar
dirclone http://123.45.67.8:8080/.hermes --concurrency 200
```

Menjalankan dari source (belum install):

```bash
cargo run --release -- http://123.45.67.8:8080/.hermes
```

## Opsi CLI

| Flag | Default | Keterangan |
|------|---------|------------|
| `--depth <N>` | *unlimited* | Batas kedalaman rekursi (root = depth 0). Omit untuk tanpa batas. |
| `--concurrency <N>` | `100` | Jumlah worker paralel (listing + file). |
| `--timeout-seconds <N>` | `60` | Total request timeout (connect + read). |
| `--connect-timeout <N>` | `8` | Timeout khusus fase TCP connect. |
| `--retries <N>` | `2` | Retry untuk transient error dan mid-stream drop. |
| `--retry-backoff-ms <N>` | `300` | Backoff dasar exponential. |
| `--max-redirects <N>` | `10` | Batas redirect HTTP. |
| `--include <GLOB>` | — | Include pattern (bisa berulang). |
| `--exclude <GLOB>` | — | Exclude pattern (bisa berulang). |
| `--user-agent <UA>` | `dirclone/0.2` | Custom User-Agent. |
| `--dry-run` | off | Simulasi — tidak menulis file. |
| `--force` | off | Overwrite file existing, abaikan resume cache. |
| `--manifest <PATH>` | `.dirclone-manifest.json` | Path manifest resume. |
| `--log-level <quiet\|info\|debug>` | `info` | Level log output. |
| `--no-progress` | off | Matikan progress bar (auto-off jika stderr bukan TTY). |

## Progress bar

Saat stderr merupakan terminal, dirclone menampilkan satu baris live:

```
⠁ [00:00:12] dirs 3/17 • files 42 (skip 1, fail 0) • 128.4 MB • in-flight 4
```

- `dirs done/total` — direktori yang selesai di-parse vs. yang sudah antre.
- `files N (skip, fail)` — file terunduh, di-skip (401/403/304), gagal.
- `bytes` — total byte yang berhasil ditulis (update mid-download).
- `in-flight` — request HTTP aktif saat ini.
- Spinner tetap ber-tick meski counter diam → sinyal "masih hidup vs. hang". Kalau counter beku >30 detik tapi spinner masih jalan, ada file besar sedang di-stream; kalau `[elapsed]` maju tapi spinner ikut diam, target-side hang → Ctrl+C untuk flush manifest.

Piped output (`dirclone … | tee log`) atau redirect ke file otomatis mematikan bar. Flag `--no-progress` memaksa off.

## Benchmarking

`probe10mb.sh` (repo root) mengukur waktu yang dibutuhkan dirclone untuk menulis `TARGET_MB` MB pertama ke disk, lalu kill prosesnya. Cepat, terukur, apple-to-apple antara konfig.

```bash
# Tolak-ukur cepat tanpa harus menyelesaikan crawl penuh
MAX_WAIT_SEC=180 ./probe10mb.sh http://target/.hermes/ --concurrency 100
# → SUMMARY target_mb=10 first_byte_ms=1053 cross_ms=48165 total_ms=... final_files=562
```

Baseline pada target uji `124.221.14.184:8080/.hermes/` (Python `SimpleHTTP/0.6`, RTT ~460 ms dari VPS, ceiling ~63 KB/s single-conn):

| Config | time to 10 MB | speedup |
|--------|---------------|---------|
| concurrency=4, timeout=20 (defaults lama) | 356 s | 1× (baseline) |
| concurrency=16 | 163 s | 2.2× |
| concurrency=32 | 66 s | 5.4× |
| concurrency=100 (**default baru**) | 48 s | **7.4×** |
| concurrency=128 | 57 s | 6.3× |

## Troubleshooting

1. **Progress bar terlihat "beku"**
   - Lihat spinner: kalau masih tick, sedang download file besar — biarkan.
   - Kalau spinner juga beku sekian menit sementara `[elapsed]` maju, target-side hang → Ctrl+C, dirclone akan flush manifest sebelum keluar; re-run untuk resume.

2. **Banyak file gagal (`Stream error … error decoding response body`)**
   - Server HTTP/1.0 (misal Python SimpleHTTP) sering putus mid-body pada file besar. `--retries 4 --timeout-seconds 120` mitigasi.

3. **Banyak file di-skip**
   - Cek target mengembalikan `401/403` (dirclone skip aman).
   - Cek pattern `--include/--exclude`.

4. **Proses berhenti dengan status 2**
   - Ada sebagian request/listing/file yang gagal, tapi crawl utama selesai.
   - Jalankan ulang untuk resume dari manifest.

5. **Ingin clone ulang total**
   - Gunakan `--force` untuk overwrite file existing dan abaikan cache.

## Validasi

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
