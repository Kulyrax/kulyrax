# Shared helpers for run.sh, lag.sh and rebuild.sh. Sourced, not run.

BACKENDS="sqlite postgres mysql redis mongodb elasticsearch neo4j file-document file-keyvalue file-graph"

# KULYRAX can point at a prebuilt binary; otherwise build this checkout once,
# so cargo's own startup is never part of a timing.
kulyrax_bin() {
  if [ -z "${KULYRAX:-}" ]; then
    cargo build --quiet --release --manifest-path ../../Cargo.toml -p kulyrax-cli >&2
    KULYRAX=../../target/release/kulyrax
  fi
  echo "$KULYRAX"
}

mappings_for() {
  case "$1" in
    sqlite) echo mappings-sqlite ;;
    postgres | mysql) echo mappings-sql ;;
    mongodb | elasticsearch | file-document) echo mappings-document ;;
    redis | file-keyvalue) echo mappings-keyvalue ;;
    neo4j | file-graph) echo mappings-graph ;;
    *) echo "unknown backend: $1 (one of: $BACKENDS)" >&2; return 2 ;;
  esac
}

dc() { docker compose -f docker-compose.yml "$@"; }

# Start an empty store for one backend: a new container and data volume
# for a networked one, an empty work directory for the rest.
fresh_store() {
  local backend=$1
  case "$backend" in
    sqlite)
      cargo run --quiet --manifest-path ../sqlite-quickstart/dbtool/Cargo.toml -- init work/bench.db
      ;;
    file-*) ;;
    *)
      stop_store "$backend"
      dc up -d --wait "$backend" >/dev/null
      ;;
  esac
  case "$backend" in
    postgres)
      dc exec -T postgres psql -q -U postgres -d bench -c \
        "CREATE TABLE users (id text PRIMARY KEY, name text, bio text, followers bigint)"
      ;;
    mysql)
      dc exec -T mysql mysql -e \
        "CREATE TABLE bench.users (id varchar(64) PRIMARY KEY, name text, bio text, followers bigint)"
      ;;
    mongodb)
      dc exec -T mongodb mongosh --quiet --eval 'rs.initiate()' >/dev/null
      for _ in $(seq 1 60); do
        [ "$(dc exec -T mongodb mongosh --quiet --eval 'rs.status().myState' 2>/dev/null)" = 1 ] && return 0
        sleep 1
      done
      echo "mongodb replica set did not become primary" >&2
      return 1
      ;;
  esac
}

# Stop and remove a networked backend's container and its data volume, so
# only one ever runs and nothing carries over to the next run.
stop_store() {
  case "$1" in
    sqlite | file-*) ;;
    *)
      dc rm -fsv "$1" >/dev/null 2>&1 || true
      docker volume rm -f "kulyrax-bench_$1-data" >/dev/null 2>&1 || true
      ;;
  esac
}

# Write stdin to results/<name>.txt and flush it to disk, so a crash loses
# at most the run in progress.
record() {
  mkdir -p results
  tee "results/$1.txt"
  sync
}

# How many entities the store holds: the number every run checks.
count_projected() {
  case "$1" in
    sqlite)
      cargo run --quiet --manifest-path ../sqlite-quickstart/dbtool/Cargo.toml -- dump work/bench.db | wc -l
      ;;
    postgres) dc exec -T postgres psql -U postgres -d bench -tAc "SELECT count(*) FROM users" ;;
    mysql) dc exec -T mysql mysql -N -e "SELECT count(*) FROM bench.users" ;;
    redis) dc exec -T redis redis-cli --scan --pattern 'users:*' --count 1000 | wc -l ;;
    mongodb)
      dc exec -T mongodb mongosh --quiet --eval 'db.getSiblingDB("bench").users.countDocuments()'
      ;;
    elasticsearch)
      curl -fs -XPOST localhost:19200/users/_refresh >/dev/null
      curl -fs localhost:19200/users/_count | sed -E 's/.*"count":([0-9]+).*/\1/'
      ;;
    neo4j)
      dc exec -T neo4j cypher-shell -u neo4j -p kulyrax-bench --format plain \
        "MATCH (n:User) RETURN count(n)" | tail -n 1
      ;;
    file-document | file-keyvalue) ls work/store/users | wc -l ;;
    file-graph) ls work/store/nodes/User | wc -l ;;
  esac
}

# gen_events <dir> <events> <per_file> <id prefix>: files 000001.ndjson,
# 000002.ndjson, … of <per_file> events each, one new entity per event
# (sequence 1), so every event is a real insert.
gen_events() {
  mkdir -p "$1"
  awk -v dir="$1" -v n="$2" -v per="$3" -v p="$4" 'BEGIN {
    for (i = 1; i <= n; i++) {
      f = sprintf("%s/%06d.ndjson", dir, int((i - 1) / per) + 1)
      printf "{\"event_id\":\"e-%s%d\",\"event_type\":\"UserRegistered\",\"payload\":\"{\\\"id\\\":\\\"%s%d\\\",\\\"name\\\":\\\"User %d\\\",\\\"email\\\":\\\"u%d@example.com\\\",\\\"bio\\\":\\\"bio %d\\\",\\\"followers\\\":%d}\",\"metadata\":{},\"version\":1,\"sequence\":1}\n", p, i, p, i, i, i, i, i > f
      if (i % per == 0) close(f)
    }
  }'
}

# Hardware line every result is recorded with.
machine() {
  powershell.exe -NoProfile -Command \
    '$c=Get-CimInstance Win32_Processor; $m=[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB); $d=(Get-PhysicalDisk | Select-Object -First 1).FriendlyName; "$($c.Name.Trim()), $m GB RAM, $d"' 2>/dev/null | tr -d '\r' \
    || uname -a
}
