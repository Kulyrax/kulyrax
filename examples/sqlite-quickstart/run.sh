#!/usr/bin/env bash
# Runs the quickstart end-to-end and diffs the projected table against
# expected/users.txt — this is also what CI runs (.github/workflows/ci.yml).
set -euo pipefail
cd "$(dirname "$0")"

rm -f app.db
rm -rf .kulyrax

DBTOOL="cargo run --quiet --manifest-path dbtool/Cargo.toml --"
KULYRAX="cargo run --quiet --manifest-path ../../Cargo.toml -p kulyrax-cli --bin kulyrax --"

$DBTOOL init app.db

$KULYRAX run --config config.yaml --mappings mappings --once

actual="$($DBTOOL dump app.db)"
expected="$(cat expected/users.txt)"

if [ "$actual" != "$expected" ]; then
  echo "MISMATCH between the projected table and expected/users.txt" >&2
  echo "--- expected ---" >&2
  echo "$expected" >&2
  echo "--- actual ---" >&2
  echo "$actual" >&2
  exit 1
fi

echo "OK — app.db.users matches expected/users.txt:"
echo "$actual"
