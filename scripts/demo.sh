#!/usr/bin/env bash
# Starts Funnel, sends it three hours of metrics and queries them with SQL over HTTP.
#
#   cargo build --release -p funnel --features lake
#   scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=target/release/funnel
PORT=${FUNNEL_DEMO_PORT:-18080}
URL=http://127.0.0.1:$PORT
WORK=$(mktemp -d)

"$BIN" --data-dir "$WORK/blocks" --lake-dir "$WORK/lake" --http "127.0.0.1:$PORT" --udp 127.0.0.1:18125 >"$WORK/funnel.log" 2>&1 &
PID=$!
cleanup() { kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

until curl -sf "$URL/healthz" >/dev/null; do sleep 0.2; done
echo "funnel is listening on $URL"

END=$(( ($(date +%s) - 900) / 3600 * 3600 ))
START=$(( END - 3 * 3600 ))
FROM=$(date -u -d "@$START" +%Y-%m-%dT%H:%M:%S)
TO=$(date -u -d "@$END" +%Y-%m-%dT%H:%M:%S)

echo
echo "== Send three hours of metrics for tenant acme =="
python3 scripts/backfill.py --url "$URL" --end "$END"

sql() { curl -s -H "x-funnel-tenant: ${2:-acme}" "$URL/sql" -d "$1"; }

echo
echo "== Wait for the closed hours to reach Iceberg =="
waited=0
until [ "$(sql "SELECT count(*) c FROM metrics_1h")" = '[{"c":27}]' ]; do
  sleep 5
  waited=$((waited + 5))
done
echo "published after ${waited} s"

echo
echo "== p99 latency per hour, in SQL =="
QUERY="SELECT date_bin(INTERVAL '1 hour', ts) AS hour, round(quantile(sketch, 0.99), 1) AS p99_ms
FROM metrics
WHERE metric = 'http.duration' AND ts >= TIMESTAMP '$FROM' AND ts < TIMESTAMP '$TO'
GROUP BY 1 ORDER BY 1"
echo "$QUERY"
echo
curl -s -D "$WORK/headers" -H "x-funnel-tenant: acme" "$URL/sql" -d "$QUERY" | python3 -m json.tool --compact | tr -d '\n' | sed 's/},/},\n /g'
echo
echo
grep -i '^x-funnel-rollup' "$WORK/headers" | tr -d '\r' | sed 's/^/response header: /' || true

echo
echo "== Requests per host, in SQL =="
sql "SELECT tags, sum(sum) AS requests FROM metrics WHERE metric = 'http.requests' AND ts >= TIMESTAMP '$FROM' AND ts < TIMESTAMP '$TO' GROUP BY 1 ORDER BY 1" \
  | python3 -m json.tool --compact | tr -d '\n' | sed 's/},/},\n /g'
echo

echo
echo "== The same total from the original /query API =="
SQL_TOTAL=$(sql "SELECT sum(sum) AS t FROM metrics WHERE metric = 'http.requests' AND ts >= TIMESTAMP '$FROM' AND ts < TIMESTAMP '$TO'")
QUERY_TOTAL=$(curl -s -H 'x-funnel-tenant: acme' "$URL/query?metric=http.requests&from=$START&to=$((END - 1))&group_by=&step=10800&agg=sum")
echo "/sql    $SQL_TOTAL"
echo "/query  $QUERY_TOTAL" | cut -c1-140

echo
echo "== Another tenant sees nothing =="
echo "tenant other: $(sql "SELECT count(*) c FROM metrics" other)"

echo
echo "== Writes and other tables are refused =="
sql "DROP TABLE metrics" | cut -c1-110
echo

echo
echo "== What is on disk =="
du -sh "$WORK/lake/warehouse/funnel/"* | sed "s#$WORK/##"
