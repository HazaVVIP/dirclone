# dirclone

Tools Rust untuk mereplikasi directory listing website ke local secara rekursif.

## Fitur

- Clone file dan folder dari directory listing sampai kedalaman terdalam.
- Parser berlapis untuk listing HTML (`<a href=...>`) dan plain text listing.
- Skip aman untuk endpoint yang mengembalikan `401` atau `403`.
- Retry + backoff untuk error transient (`5xx`, timeout, koneksi putus).
- Kontrol redirect (`--max-redirects`).
- Filter path dengan include/exclude glob.
- Proteksi path traversal saat mapping URL ke local path.
- Mode `--dry-run` untuk simulasi clone.
- Concurrent download worker (`--concurrency`).
- Resume berbasis manifest lokal dan mode `--force` untuk overwrite.
- Ringkasan hasil clone dan exit code:
  - `0` = sukses penuh
  - `2` = sukses parsial (ada file/listing gagal)
  - `1` = fatal error

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run -- <URL_DIRECTORY_ROOT/> <OUTPUT_DIR> [OPTIONS]
```

Contoh:

```bash
cargo run -- http://123.23.34.555/.hermes/ /tmp/hermes-clone
```

## Opsi CLI

- `--timeout-seconds <N>`: timeout request HTTP (default `20`)
- `--user-agent <UA>`: custom user-agent request
- `--retries <N>`: jumlah retry untuk transient error (default `2`)
- `--retry-backoff-ms <N>`: backoff dasar retry dalam milidetik (default `300`)
- `--max-redirects <N>`: batas redirect HTTP (default `10`)
- `--include <GLOB>`: include pattern (repeatable)
- `--exclude <GLOB>`: exclude pattern (repeatable)
- `--dry-run`: simulasi tanpa menulis file
- `--concurrency <N>`: jumlah worker download paralel (default `4`)
- `--force`: overwrite file existing dan abaikan resume cache
- `--manifest <PATH>`: path/filename manifest resume (default `.dirclone-manifest.json`)
- `--log-level <quiet|info|debug>`: level log output

## Troubleshooting

1. **Banyak file di-skip**
   - Cek apakah target mengembalikan `401/403`.
   - Cek pattern `--include/--exclude`.

2. **Proses berhenti dengan status 2**
   - Ada sebagian request/listing/file yang gagal, tapi proses utama tetap jalan.
   - Jalankan ulang untuk resume dari manifest.

3. **Ingin clone ulang total**
   - Gunakan `--force` untuk overwrite file existing.

## Validasi

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
