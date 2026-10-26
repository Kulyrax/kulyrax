#!/usr/bin/env bash
# Rebuild while ingesting, on Postgres: rebuild a projection from a full
# history (`kulyrax run --once` into an empty `rebuild` database) while a
# second, live `kulyrax run` keeps projecting new events into `bench` at a
# steady rate. Reports the rebuild's throughput and the live lag during it;
# compare the lag with `lag.sh postgres <rate>` run on its own.
#
#   bash rebuild.sh [history events] [live rate] [live seconds]   # 100000, 15, 180
set -euo pipefail
cd "$(dirname "$0")"
. ./lib.sh

n=${1:-100000}
rate=${2:-15}
seconds=${3:-180}

bin=$(kulyrax_bin)
cargo build --quiet --release --manifest-path benchtool/Cargo.toml
per_file=$(( rate / 10 > 0 ? rate / 10 : 1 ))
live_n=$(( rate * seconds ))

rm -rf work
gen_events work/rebuild-events "$n" 1000 u
mkdir -p work/events
trap 'stop_store postgres' EXIT
fresh_store postgres
dc exec -T postgres psql -q -U postgres -d bench -c "CREATE DATABASE rebuild"
dc exec -T postgres psql -q -U postgres -d rebuild -c \
  "CREATE TABLE users (id text PRIMARY KEY, name text, bio text, followers bigint)"

# Paths in a config resolve relative to the config file, which lives in work/.
sed -e 's|/bench$|/rebuild|' -e 's|\./work/events|./rebuild-events|' \
  -e 's|\./work/state|./rebuild-state|' postgres.yaml > work/rebuild.yaml
cp routing.yaml work/

start=$(date +%s.%N)
"$bin" run --config work/rebuild.yaml --mappings mappings-sql --once > work/rebuild.log &
rebuild_pid=$!

live=$(benchtool/target/release/benchmark-benchtool lag \
  --dir work/events --checkpoint work/state/bench.json \
  --events "$live_n" --per-file "$per_file" --rate "$rate" -- \
  "$bin" run --config postgres.yaml --mappings mappings-sql --poll-interval-ms 100)

overlapped=yes
kill -0 "$rebuild_pid" 2>/dev/null || overlapped="NO (the rebuild finished first; raise the history size)"
wait "$rebuild_pid"
end=$(date +%s.%N)

rebuilt=$(dc exec -T postgres psql -U postgres -d rebuild -tAc "SELECT count(*) FROM users" | tr -d '[:space:]')
live_found=$(count_projected postgres | tr -d '[:space:]')
[ "$rebuilt" = "$n" ] || { echo "rebuild: expected $n, found $rebuilt" >&2; exit 1; }
[ "$live_found" = "$(( live_n + per_file ))" ] || { echo "live: expected $(( live_n + per_file )), found $live_found" >&2; exit 1; }

{
  awk -v n="$n" -v s="$start" -v e="$end" 'BEGIN {
    printf "postgres rebuild: %d events in %.2f s = %.0f events/s (%d rows verified)\n", n, e - s, n / (e - s), n
  }'
  echo "live lag during rebuild at $rate events/s for ${seconds} s (live run inside the rebuild: $overlapped): $live"
} | record postgres-rebuild
