#!/usr/bin/env bash
# The task's "single documented demo command" — starts the stack, waits for
# it to be healthy, processes both sample snapshots one at a time (dropping
# the second only once the first has settled, matching the one-snapshot-at-
# a-time contract this producer itself enforces), and prints where to look.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p data/incoming data/archive data/failed

echo "Starting the stack (postgres, mongo, elasticsearch, kulyrax-ingest, producer)..."
docker compose up -d --build

echo "Waiting for services to become healthy..."
for svc in postgres mongo elasticsearch; do
  for _ in $(seq 1 60); do
    status=$(docker compose ps -q "$svc" | xargs -r docker inspect -f '{{.State.Health.Status}}' 2>/dev/null || echo "starting")
    [ "$status" = "healthy" ] && break
    sleep 2
  done
done

echo "Dropping snapshot 1 (2026-01-01.csv) — expect 4x VehicleCreated..."
cp samples/2026-01-01.csv data/incoming/2026-01-01.csv.tmp
mv data/incoming/2026-01-01.csv.tmp data/incoming/2026-01-01.csv

sleep 10

echo "Dropping snapshot 2 (2026-01-02.csv) — expect PriceChanged(v1001), Updated(v1002), Created(v1005), Sold(v1004)..."
cp samples/2026-01-02.csv data/incoming/2026-01-02.csv.tmp
mv data/incoming/2026-01-02.csv.tmp data/incoming/2026-01-02.csv

sleep 10

echo
echo "Done. Check progress with: docker compose logs -f producer"
echo "Verify the projections with: ./scripts/verify.sh"
