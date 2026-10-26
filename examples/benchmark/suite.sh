#!/usr/bin/env bash
# The full benchmark: for each backend, sustained throughput, then lag at half
# that throughput; after Postgres, the rebuild-while-ingesting run.
#
#   bash suite.sh
#
# MySQL and Elasticsearch run 10,000 events, every other backend 100,000.
# Only one backend's container runs at a time, and each script stops its
# container when it exits. Every result is written to results/ (and flushed)
# as soon as that run finishes, so a crash loses at most the run in progress.
set -uo pipefail
cd "$(dirname "$0")"
. ./lib.sh

order="file-document file-keyvalue file-graph redis sqlite postgres mongodb neo4j elasticsearch mysql"
events_for() {
  case "$1" in
    mysql | elasticsearch) echo 10000 ;;
    *) echo 100000 ;;
  esac
}

rm -rf results
mkdir -p results
machine | record machine

# One binary for the whole suite, built before any timing. Nothing compiles
# while the suite runs.
cargo build --quiet --release --manifest-path ../../Cargo.toml -p kulyrax-cli
cargo build --quiet --release --manifest-path benchtool/Cargo.toml
mkdir -p ../../target/bench
cp ../../target/release/kulyrax.exe ../../target/bench/ 2>/dev/null || cp ../../target/release/kulyrax ../../target/bench/
export KULYRAX=../../target/bench/kulyrax
git rev-parse --short HEAD | record commit

dc down -v >/dev/null 2>&1 || true

for b in $order; do
  echo "=== $b $(date -u +%FT%TZ)"
  n=$(events_for "$b")
  bash run.sh "$b" "$n" 1000 || { echo "$b: throughput run FAILED"; continue; }
  eps=$(sed -nE 's/.* = ([0-9]+) events\/s.*/\1/p' "results/$b.txt")
  rate=$(( eps / 2 > 0 ? eps / 2 : 1 ))
  bash lag.sh "$b" "$rate" 180 100 || echo "$b: lag run FAILED"
  if [ "$b" = postgres ]; then
    bash rebuild.sh "$n" "$rate" 180 || echo "rebuild run FAILED"
  fi
done
echo "=== done $(date -u +%FT%TZ)"
