#!/usr/bin/env bash
# Sustained throughput: time `kulyrax run --once` projecting N UserRegistered
# events (one new entity each, split into files of PER_FILE events) into an
# empty store, check every entity landed, and print the per-stage timings.
#
#   bash run.sh <backend> [events] [per_file]    # defaults: 100000, 1000
#   bash run.sh all [events] [per_file]          # every backend in turn
#
# Backends: sqlite postgres mysql redis mongodb elasticsearch neo4j
#           file-document file-keyvalue file-graph
# Networked backends run in Docker (docker-compose.yml); each run recreates
# the container, so the store starts empty.
set -euo pipefail
cd "$(dirname "$0")"
. ./lib.sh

backend=${1:?usage: run.sh <backend>|all [events] [per_file]}
n=${2:-100000}
per_file=${3:-1000}

if [ "$backend" = all ]; then
  for b in $BACKENDS; do bash "$0" "$b" "$n" "$per_file" || echo "$b: FAILED" >&2; done
  exit 0
fi

mappings=$(mappings_for "$backend")
bin=$(kulyrax_bin)

rm -rf work
gen_events work/events "$n" "$per_file" u
trap 'stop_store "$backend"' EXIT
fresh_store "$backend"

start=$(date +%s.%N)
"$bin" run --config "$backend.yaml" --mappings "$mappings" --once > work/run.log
end=$(date +%s.%N)

found=$(count_projected "$backend" | tr -d '[:space:]')
if [ "$found" != "$n" ]; then
  echo "$backend: expected $n entities, found $found (see work/run.log)" >&2
  exit 1
fi

{
  awk -v n="$n" -v per="$per_file" -v s="$start" -v e="$end" -v b="$backend" 'BEGIN {
  t = e - s
  printf "%s: %d events (%d per file) in %.2f s = %.0f events/s (%d entities verified)\n", b, n, per, t, n / t, n
}'
  sed -n '/^Time per stage/,/^---/p' work/run.log | sed '$d'
} | record "$backend"
