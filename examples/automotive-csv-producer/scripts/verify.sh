#!/usr/bin/env bash
# Queries all three destinations for the vehicles the sample walkthrough
# produces. Run from this directory after `docker compose up` and after
# both sample snapshots have been dropped into ./data/incoming (see README).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== PostgreSQL (kulyrax database, vehicles table) ==="
docker compose exec -T postgres psql -U postgres -d kulyrax -c \
  "SELECT provider_vehicle_id, price, mileage, sold FROM vehicles ORDER BY provider_vehicle_id;"

echo
echo "=== MongoDB (kulyrax database, vehicles collection) ==="
docker compose exec -T mongo mongosh --quiet kulyrax --eval \
  'db.vehicles.find({}, {_id:1, price:1, mileage:1, sold:1}).sort({_id:1}).forEach(d => print(JSON.stringify(d)))'

echo
echo "=== Elasticsearch (vehicles index) ==="
curl -s "http://localhost:9200/vehicles/_search?size=50&_source=provider_vehicle_id,price,mileage,sold" \
  | python3 -m json.tool 2>/dev/null || curl -s "http://localhost:9200/vehicles/_search?size=50"
