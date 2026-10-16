#!/usr/bin/env bash
# Starts `kulyrax ingest` in the background, streams two events into it with
# the reference Rust producer, then diffs the projected table against
# expected/users.txt. Not wired into CI (background server + port not worth
# the flake risk there) but fully self-contained locally — no external infra.
set -euo pipefail
cd "$(dirname "$0")"

PORT=50061
LISTEN="tcp://127.0.0.1:${PORT}"

rm -f app.db
SERVER_LOG="$(mktemp)"

# Build and invoke the `kulyrax` binary directly (not via `cargo run`) so
# $! below is the server's own pid, not a `cargo` wrapper's — reliable
# shutdown needs a direct child, since signalling a `cargo run` process
# doesn't reliably reach the binary it spawns.
cargo build --quiet --manifest-path ../../Cargo.toml -p kulyrax-cli --bin kulyrax
KULYRAX_BIN="../../target/debug/kulyrax"
[ -f "${KULYRAX_BIN}.exe" ] && KULYRAX_BIN="${KULYRAX_BIN}.exe"

DBTOOL="cargo run --quiet --manifest-path dbtool/Cargo.toml --"
PRODUCER="cargo run --quiet --manifest-path producer/Cargo.toml --"

$DBTOOL init app.db

server_pid=""
stop_server() {
  [ -n "$server_pid" ] || return 0
  kill -0 "$server_pid" 2>/dev/null || { server_pid=""; return 0; }
  kill -INT "$server_pid" 2>/dev/null || true
  for _ in $(seq 1 20); do
    kill -0 "$server_pid" 2>/dev/null || { server_pid=""; return 0; }
    sleep 0.1
  done
  # Ctrl+C-style shutdown isn't always deliverable to a backgrounded,
  # console-detached process — fall back to a hard kill.
  kill -9 "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
}
cleanup() {
  stop_server
  rm -f "$SERVER_LOG"
}
trap cleanup EXIT

"$KULYRAX_BIN" ingest --config config.yaml --mappings mappings --listen "$LISTEN" >"$SERVER_LOG" 2>&1 &
server_pid=$!

for _ in $(seq 1 100); do
  if grep -q "listening on" "$SERVER_LOG" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    echo "kulyrax ingest exited before it started listening:" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 0.1
done

$PRODUCER "http://127.0.0.1:${PORT}"

stop_server

actual="$($DBTOOL dump app.db)"
expected="$(cat expected/users.txt)"

if [ "$actual" != "$expected" ]; then
  echo "MISMATCH between the projected table and expected/users.txt" >&2
  echo "--- expected ---" >&2
  echo "$expected" >&2
  echo "--- actual ---" >&2
  echo "$actual" >&2
  echo "--- server log ---" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

echo "OK — app.db.users matches expected/users.txt:"
echo "$actual"
