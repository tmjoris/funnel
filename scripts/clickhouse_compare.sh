#!/usr/bin/env bash
# Stores the raw points of the generated workload in ClickHouse with four
# codec setups and prints bytes per point, plus exact percentiles to check
# Funnel's answers against (see the query_check example).
#
#   cargo build --release -p funnel --examples
#   scripts/clickhouse_compare.sh [minutes]
set -euo pipefail

MINUTES=${1:-10}
IMAGE=clickhouse/clickhouse-server:25.8.33.6
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CH="docker exec -i funnel-ch clickhouse-client"

docker rm -f funnel-ch >/dev/null 2>&1 || true
docker run -d --name funnel-ch --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 "$IMAGE" >/dev/null
until $CH -q "SELECT 1" >/dev/null 2>&1; do sleep 1; done

schema() { # table, metric/tags codec, ts codec, value codec
  $CH -q "CREATE TABLE $1 (
      metric LowCardinality(String) $2,
      tags Map(LowCardinality(String), String) $2,
      ts DateTime64(3) $3,
      value Float64 $4)
    ENGINE = MergeTree ORDER BY (metric, tags, ts)"
}
schema raw_lz4 "" "" ""
schema raw_zstd "CODEC(ZSTD(3))" "CODEC(ZSTD(3))" "CODEC(ZSTD(3))"
schema raw_gorilla "CODEC(ZSTD(3))" "CODEC(DoubleDelta, ZSTD(3))" "CODEC(Gorilla, ZSTD(3))"
schema raw_best "CODEC(ZSTD(3))" "CODEC(DoubleDelta, ZSTD(3))" "CODEC(ZSTD(3))"

"$ROOT/target/release/examples/export_raw" "$MINUTES" | $CH -q "INSERT INTO raw_best FORMAT TSV"
for t in raw_lz4 raw_zstd raw_gorilla; do $CH -q "INSERT INTO $t SELECT * FROM raw_best"; done
for t in raw_lz4 raw_zstd raw_gorilla raw_best; do $CH -q "OPTIMIZE TABLE $t FINAL"; done

$CH -q "SELECT table, sum(rows) AS points, sum(bytes_on_disk) AS disk,
           round(sum(bytes_on_disk) / sum(rows), 3) AS bytes_per_point
        FROM system.parts WHERE active AND database = 'default'
        GROUP BY table ORDER BY bytes_per_point FORMAT PrettyCompactMonoBlock"

# Exact percentiles at DDSketch's rank, floor(q * (n - 1)), 0-based.
$CH -q "WITH arraySort(groupArray(value)) AS v, length(v) AS n
        SELECT n,
          arrayElement(v, toUInt64(floor(0.5 * (n - 1))) + 1) AS p50,
          arrayElement(v, toUInt64(floor(0.99 * (n - 1))) + 1) AS p99,
          arrayElement(v, toUInt64(floor(0.999 * (n - 1))) + 1) AS p999
        FROM raw_best WHERE metric = 'http.duration' AND tags['route'] = '/r3'
        FORMAT PrettyCompactMonoBlock"

docker rm -f funnel-ch >/dev/null
