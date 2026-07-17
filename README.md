# dirclone

Tools Rust untuk mereplikasi directory listing website ke local secara rekursif.

## Fitur

- Clone file dan folder dari directory listing sampai kedalaman terdalam.
- Mendukung listing berbasis HTML (`<a href=...>`) dan plain text listing.
- Skip aman untuk endpoint yang mengembalikan `401` atau `403`.
- Hanya menyalin konten di bawah root URL yang diberikan.

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run -- <URL_DIRECTORY_ROOT/> <OUTPUT_DIR>
```

Contoh:

```bash
cargo run -- http://123.23.34.555/.hermes/ /tmp/hermes-clone
```

Opsi tambahan:

- `--timeout-seconds <N>`: timeout request HTTP (default `20`)
- `--user-agent <UA>`: custom user-agent request
