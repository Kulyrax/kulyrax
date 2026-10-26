#!/usr/bin/env bash
# Time `kulyrax run --once` projecting N UserRegistered events into SQLite or
# Postgres, from a clean table and a clean source checkpoint.
#
#   bash run.sh sqlite   [events]   # default 100000
#   bash run.sh postgres [events]   # needs DATABASE_URL (default: local postgres)
#
# KULYRAX can point at a prebuilt binary; otherwise a release build is used.
set -euo pipefail
cd "$(dirname "$0")"

store=${1:?usage: run.sh sqlite|postgres [events]}
n=${2:-100000}
KULYRAX=${KULYRAX:-"cargo run --quiet --release --manifest-path ../../Cargo.toml -p kulyrax-cli --bin kulyrax --"}

rm -rf events .kulyrax bench.db
mkdir events
# One entity per event, sequence 1: every event is a real insert.
awk -v n="$n" 'BEGIN {
  for (i = 1; i <= n; i++)
    printf "{\"event_id\":\"e%d\",\"event_type\":\"UserRegistered\",\"payload\":\"{\\\"id\\\":\\\"u%d\\\",\\\"name\\\":\\\"User %d\\\",\\\"email\\\":\\\"u%d@example.com\\\",\\\"bio\\\":\\\"bio %d\\\",\\\"followers\\\":%d}\",\"metadata\":{},\"version\":1,\"sequence\":1}\n", i, i, i, i, i, i
}' > events/events.ndjson

case "$store" in
  sqlite)
    DBTOOL="cargo run --quiet --manifest-path ../sqlite-quickstart/dbtool/Cargo.toml --"
    DB=bench.db
    ;;
  postgres)
    export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
    DBTOOL="cargo run --quiet --manifest-path ../postgres/dbtool/Cargo.toml --"
    DB="$DATABASE_URL"
    ;;
  *) echo "unknown store: $store (sqlite or postgres)" >&2; exit 2 ;;
esac
$DBTOOL init "$DB"

start=$(date +%s.%N)
$KULYRAX run --config "$store.yaml" --mappings "mappings-$store" --once > run.log
end=$(date +%s.%N)

rows=$($DBTOOL dump "$DB" | wc -l)
if [ "$rows" -ne "$n" ]; then
  echo "expected $n rows in users, found $rows" >&2
  exit 1
fi
awk -v n="$n" -v s="$start" -v e="$end" -v store="$store" 'BEGIN {
  t = e - s
  printf "%s: %d events in %.2f s = %.0f events/s (%d rows verified)\n", store, n, t, n / t, n
}'
