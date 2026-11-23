#!/usr/bin/env python3
"""Runs dashboard queries in Cloudflare Basin SQL against Funnel's raw table and its rollups.

Basin SQL bills by bytes scanned, so for each dashboard this records the bytes and files it read
from the 10-second table and from the coarsest rollup that gives the same answer, the median
time over several runs, and whether both answers match. Uses the `bench` tenant written by
`cargo run --release -p funnel-lake --example basin`.

    BASIN_TOKEN=... CF_ACCOUNT_ID=... BASIN_BUCKET=... scripts/basin_sql_bench.py --days 7
"""
import argparse
import datetime as dt
import json
import os
import statistics
import time
import urllib.error
import urllib.request

EPOCH = dt.datetime(2026, 1, 1)
TENANT = "bench"
# https://developers.cloudflare.com/basin-sql/platform/pricing/
PRICE_PER_TB = 2.50
MIN_BYTES = 10_000_000


def stamp(hours):
    return "TIMESTAMP '" + (EPOCH + dt.timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S") + "'"


def dashboards(days):
    end = days * 24

    def where(metric, start):
        return (f"tenant = '{TENANT}' AND metric = '{metric}' "
                f"AND ts >= {stamp(start)} AND ts < {stamp(end)}")

    return [
        ("request total, last hour, per minute", "metrics_1m",
         "SELECT date_bin(INTERVAL '60 seconds', ts) AS t, sum(sum) AS v FROM funnel.{table} "
         f"WHERE {where('http.requests', end - 1)} GROUP BY 1 ORDER BY 1 LIMIT 10000"),
        ("request total, last day, per 10 minutes", "metrics_10m",
         "SELECT date_bin(INTERVAL '600 seconds', ts) AS t, sum(sum) AS v FROM funnel.{table} "
         f"WHERE {where('http.requests', end - 24)} GROUP BY 1 ORDER BY 1 LIMIT 10000"),
        (f"request total, last {days} days, per hour", "metrics_1h",
         "SELECT date_bin(INTERVAL '3600 seconds', ts) AS t, sum(sum) AS v FROM funnel.{table} "
         f"WHERE {where('http.requests', 0)} GROUP BY 1 ORDER BY 1 LIMIT 10000"),
        (f"peak queue per host, last {days} days", "metrics_1h",
         "SELECT tags, max(max) AS v FROM funnel.{table} "
         f"WHERE {where('queue.depth', 0)} GROUP BY 1 ORDER BY 1 LIMIT 10000"),
    ]


def query(api, token, sql):
    req = urllib.request.Request(
        api,
        data=json.dumps({"query": sql}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            # Cloudflare's bot filter rejects Python's default User-Agent (error 1010).
            "User-Agent": "funnel-bench/0.1",
        },
    )
    start = time.monotonic()
    try:
        body = json.load(urllib.request.urlopen(req, timeout=300))
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Basin SQL HTTP {e.code}: {e.read()[:300]!r}\n{sql}")
    ms = (time.monotonic() - start) * 1000
    if not body.get("success"):
        raise SystemExit(f"Basin SQL error: {body.get('errors')}\n{sql}")
    return ms, body["result"]["rows"], body["result"]["metrics"]


def same(a, b):
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if x.keys() != y.keys():
            return False
        for k in x:
            u, v = x[k], y[k]
            if isinstance(u, float) or isinstance(v, float):
                if abs(float(u) - float(v)) > 1e-6 * max(1.0, abs(float(u))):
                    return False
            elif u != v:
                return False
    return True


def cost_per_1000(nbytes):
    return max(nbytes, MIN_BYTES) / 1e12 * PRICE_PER_TB * 1000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--json", default="basin-sql.json")
    args = ap.parse_args()
    api = ("https://api.sql.cloudflarestorage.com/api/v1/accounts/"
           f"{os.environ['CF_ACCOUNT_ID']}/basin-sql/query/{os.environ['BASIN_BUCKET']}")
    token = os.environ["BASIN_TOKEN"]

    results = []
    for name, rollup, template in dashboards(args.days):
        entry = {"dashboard": name, "rollup": rollup}
        answers = {}
        for side, table in (("raw", "metrics"), ("rollup", rollup)):
            sql = template.format(table=table)
            times, metrics, rows = [], None, None
            for _ in range(args.runs):
                ms, rows, metrics = query(api, token, sql)
                times.append(ms)
            answers[side] = rows
            entry[side] = {
                "table": table,
                "median_ms": round(statistics.median(times)),
                "bytes_scanned": metrics["bytes_scanned"],
                "files_scanned": metrics["files_scanned"],
                "rows": len(rows),
            }
        entry["same_answer"] = same(answers["raw"], answers["rollup"])
        results.append(entry)

    with open(args.json, "w") as f:
        json.dump(results, f, indent=2)

    print("| dashboard | rollup | raw MB scanned | rollup MB scanned | less data | raw ms | rollup ms "
          "| raw $ per 1,000 | rollup $ per 1,000 | same answer |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for e in results:
        raw, roll = e["raw"], e["rollup"]
        print(f"| {e['dashboard']} | {e['rollup']} "
              f"| {raw['bytes_scanned'] / 1e6:.1f} | {roll['bytes_scanned'] / 1e6:.2f} "
              f"| {raw['bytes_scanned'] / max(roll['bytes_scanned'], 1):.0f}x "
              f"| {raw['median_ms']} | {roll['median_ms']} "
              f"| {cost_per_1000(raw['bytes_scanned']):.3f} | {cost_per_1000(roll['bytes_scanned']):.3f} "
              f"| {'yes' if e['same_answer'] else 'NO'} |")
    if not all(e["same_answer"] for e in results):
        raise SystemExit("a rollup gave a different answer from the raw table")


if __name__ == "__main__":
    main()
