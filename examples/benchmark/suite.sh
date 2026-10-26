#!/usr/bin/env bash
# The full benchmark: for every backend, sustained throughput over N events,
# then lag at half that throughput; then Postgres rebuild-while-ingesting.
# Results land in results/. Slowest backends last, so a long run can be cut
# short and still leave the others measured.
#
#   bash suite.sh [events]    # default 100000
set -uo pipefail
cd "$(dirname "$0")"
. ./lib.sh

n=${1:-100000}
order="file-document file-keyvalue file-graph redis sqlite postgres mongodb neo4j elasticsearch mysql"

mkdir -p results
machine > results/machine.txt
for b in $order; do
  echo "=== $b $(date -u +%FT%TZ)"
  bash run.sh "$b" "$n" 1000 || { echo "$b: throughput run FAILED"; continue; }
  eps=$(sed -nE 's/.* = ([0-9]+) events\/s.*/\1/p' "results/$b.txt")
  rate=$(( eps / 2 > 0 ? eps / 2 : 1 ))
  bash lag.sh "$b" "$rate" 180 100 || echo "$b: lag run FAILED"
  if [ "$b" = postgres ]; then
    bash rebuild.sh "$n" "$rate" 180 || echo "rebuild run FAILED"
  fi
done
echo "=== done $(date -u +%FT%TZ)"
