#!/usr/bin/env bash
# probe10mb.sh — measure "wall-clock to reach 10 MB downloaded" for dirclone.
# Runs on the VPS (invoke via ssh). Args pass through to dirclone.
#
# Why: full crawls take unbounded time on a real target. A fixed-byte tape
# measure gives us a repeatable, target-comparable number in seconds.
set -euo pipefail

TARGET_MB=${TARGET_MB:-10}
TARGET_BYTES=$((TARGET_MB * 1024 * 1024))
POLL_MS=250
MAX_WAIT_SEC=${MAX_WAIT_SEC:-600}

export PATH="$HOME/.cargo/bin:$PATH"
WORK=$(mktemp -d)
cd "$WORK"

# Start dirclone in the background. --no-progress so the process doesn't
# suppress its own summary line when killed.
dirclone "$@" --no-progress >/tmp/dirclone.stderr 2>&1 &
PID=$!

START_NS=$(date +%s%N)
FIRST_BYTE_NS=0
CROSS_NS=0
LAST_SIZE=0

# Names of output dirs to sum. dirclone auto-derives the output name from the
# URL if user didn't pass one, so we sum whatever appears under $WORK.
size_bytes() {
    # Sum every regular file under the working dir, including files inside
    # dot-directories (`./.hermes/...`). We can't use `du -s .` because that
    # includes 4-KB inode overhead for empty dirs. `find` + `stat` gives the
    # true payload size and quietly ignores files that vanish mid-crawl.
    find . -type f -not -name '.dirclone-manifest.json' -printf '%s\n' 2>/dev/null \
        | awk '{s+=$1} END {print s+0}'
}

# Pipefail is nice for the dirclone launch but the poll loop uses `du | awk`
# whose left side legitimately fails when the target dir is empty. Turn it off
# once we're past the setup phase.
set +o pipefail

while true; do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "note: dirclone exited before reaching ${TARGET_MB} MB" >&2
        break
    fi

    SZ=$(size_bytes)
    NOW_NS=$(date +%s%N)
    ELAPSED_MS=$(( (NOW_NS - START_NS) / 1000000 ))

    if [ "$FIRST_BYTE_NS" = "0" ] && [ "$SZ" -gt 0 ]; then
        FIRST_BYTE_NS=$NOW_NS
        FIRST_BYTE_MS=$(( (FIRST_BYTE_NS - START_NS) / 1000000 ))
        echo "[t=${FIRST_BYTE_MS}ms] first bytes on disk"
    fi

    if [ "$SZ" -ge "$TARGET_BYTES" ]; then
        CROSS_NS=$NOW_NS
        CROSS_MS=$(( (CROSS_NS - START_NS) / 1000000 ))
        echo "[t=${CROSS_MS}ms] reached ${TARGET_MB} MB (actual: $SZ bytes)"
        break
    fi

    if [ "$ELAPSED_MS" -gt $((MAX_WAIT_SEC * 1000)) ]; then
        echo "note: timeout after ${MAX_WAIT_SEC}s; last size=$SZ bytes" >&2
        break
    fi

    LAST_SIZE=$SZ
    sleep 0.$(printf "%03d" $POLL_MS)
done

# Kill dirclone if still running.
kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true

# Emit machine-readable summary on the last line.
FINAL_SIZE=$(size_bytes)
FINAL_FILES=$(find . -type f -not -name '.dirclone-manifest.json' 2>/dev/null | wc -l)
FINAL_NS=$(date +%s%N)
FINAL_MS=$(( (FINAL_NS - START_NS) / 1000000 ))

# TSV summary for easy comparison across runs.
first_byte_ms() {
    if [ "$FIRST_BYTE_NS" = "0" ]; then echo "NA"
    else echo $(( (FIRST_BYTE_NS - START_NS) / 1000000 ))
    fi
}
cross_ms() {
    if [ "$CROSS_NS" = "0" ]; then echo "NA"
    else echo $(( (CROSS_NS - START_NS) / 1000000 ))
    fi
}
printf "SUMMARY\ttarget_mb=%d\tfirst_byte_ms=%s\tcross_ms=%s\ttotal_ms=%d\tfinal_bytes=%d\tfinal_files=%s\n" \
    "$TARGET_MB" \
    "$(first_byte_ms)" \
    "$(cross_ms)" \
    "$FINAL_MS" \
    "$FINAL_SIZE" \
    "$FINAL_FILES"

cd /tmp
rm -rf "$WORK"
