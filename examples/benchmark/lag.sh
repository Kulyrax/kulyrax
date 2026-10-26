#!/usr/bin/env bash
# Projection lag under a steady load: run `kulyrax run` continuously while
# benchtool writes RATE events/s into the watched directory, and report how
# long each file waits from being published to its batch being committed.
#
#   bash lag.sh <backend> <rate> [seconds] [poll_ms]   # defaults: 180 s, 100 ms
#
# Files hold rate/10 events (at least one), so a new file appears about every
# 100 ms. Lag includes up to one poll interval of idle wait, as it would in
# production: `kulyrax run` sleeps poll_ms whenever it is caught up.
set -euo pipefail
cd "$(dirname "$0")"
. ./lib.sh

backend=${1:?usage: lag.sh <backend> <rate> [seconds] [poll_ms]}
rate=${2:?usage: lag.sh <backend> <rate> [seconds] [poll_ms]}
seconds=${3:-180}
poll_ms=${4:-100}

mappings=$(mappings_for "$backend")
bin=$(kulyrax_bin)
per_file=$(( rate / 10 > 0 ? rate / 10 : 1 ))
n=$(( rate * seconds ))

cargo build --quiet --release --manifest-path benchtool/Cargo.toml
rm -rf work
mkdir -p work/events
fresh_store "$backend"

result=$(benchtool/target/release/benchmark-benchtool lag \
  --dir work/events --checkpoint work/state/bench.json \
  --events "$n" --per-file "$per_file" --rate "$rate" -- \
  "$bin" run --config "$backend.yaml" --mappings "$mappings" --poll-interval-ms "$poll_ms")

# benchtool's warm-up file adds one file's worth of entities.
expected=$(( n + per_file ))
found=$(count_projected "$backend" | tr -d '[:space:]')
if [ "$found" != "$expected" ]; then
  echo "$backend: expected $expected entities, found $found" >&2
  exit 1
fi

mkdir -p results
echo "$backend lag at $rate events/s for ${seconds} s (poll ${poll_ms} ms, $per_file events/file, $expected entities verified): $result" \
  | tee "results/$backend-lag.txt"
