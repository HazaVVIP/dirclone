#!/usr/bin/env bash
# Sync local dirclone source to Haza's VPS, build, run tests, and install.
# Usage: ./dev-sync.sh [--no-test] [--no-install]
#
# WHY: Windows Application Control (WDAC) refuses to execute cargo-emitted
# build scripts under this user profile, so all Rust work happens on the VPS.
# The VPS keeps a fresh checkout at ~/dirclone-dev; the original ~/dirclone
# is left as-is.
set -euo pipefail

HOST="Haza@38.47.85.195"
REMOTE_DIR="/home/Haza/dirclone-dev"
LOCAL_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

run_tests=1
do_install=1
for arg in "$@"; do
    case "$arg" in
        --no-test)    run_tests=0 ;;
        --no-install) do_install=0 ;;
        *) echo "unknown flag: $arg" >&2; exit 2 ;;
    esac
done

echo "==> rsync $LOCAL_DIR/ -> $HOST:$REMOTE_DIR/"
rsync -avz --delete \
    --exclude='target/' \
    --exclude='test123/' \
    --exclude='.dirclone-manifest.json' \
    "$LOCAL_DIR/" "$HOST:$REMOTE_DIR/"

remote_cmd="export PATH=\"\$HOME/.cargo/bin:\$PATH\"
cd $REMOTE_DIR
echo '==> cargo build --release'
cargo build --release"
if [ "$run_tests" -eq 1 ]; then
    remote_cmd="$remote_cmd
echo '==> cargo test --release'
cargo test --release"
fi
if [ "$do_install" -eq 1 ]; then
    remote_cmd="$remote_cmd
echo '==> installing to ~/.cargo/bin/dirclone'
install -m 755 target/release/dirclone \$HOME/.cargo/bin/dirclone
dirclone --version"
fi

ssh "$HOST" "$remote_cmd"
