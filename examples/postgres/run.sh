#!/usr/bin/env bash
# Requires a reachable Postgres — export DATABASE_URL or accept the default
# below (a local dev Postgres on the standard port). Not run by the default
# `cargo test` / CI matrix (see .github/workflows/ci.yml's `test-postgres`
# job, which sets DATABASE_URL to its service container and runs this).
set -euo pipefail
cd "$(dirname "$0")"

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"

DBTOOL="cargo run --quiet --manifest-path dbtool/Cargo.toml --"
KULYRAX="cargo run --quiet --manifest-path ../../Cargo.toml -p kulyrax-cli --bin kulyrax --"

rm -rf .kulyrax

$DBTOOL init "$DATABASE_URL"

$KULYRAX run --config config.yaml --mappings mappings --once

actual="$($DBTOOL dump "$DATABASE_URL")"
expected="$(cat expected/users.txt)"

if [ "$actual" != "$expected" ]; then
  echo "MISMATCH between the projected table and expected/users.txt" >&2
  echo "--- expected ---" >&2
  echo "$expected" >&2
  echo "--- actual ---" >&2
  echo "$actual" >&2
  exit 1
fi

echo "OK — users matches expected/users.txt:"
echo "$actual"
