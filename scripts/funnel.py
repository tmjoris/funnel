#!/usr/bin/env python3
"""A small command-line client for Funnel that prints tables instead of JSON.

    scripts/funnel.py query http.duration --agg p99 --by host --step 600 --from 13:00 --to 16:00
    scripts/funnel.py sql "SELECT tags, max(max) AS peak FROM metrics WHERE metric = 'queue.depth' GROUP BY 1"
    scripts/funnel.py stats

Times are UTC. --from and --to take HH:MM for today, or unix seconds.
"""
import argparse
import datetime as dt
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

COLOR = sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def paint(code, text):
    return f"\033[{code}m{text}\033[0m" if COLOR else text


def dim(t):
    return paint("2", t)


def bold(t):
    return paint("1", t)


def green(t):
    return paint("32", t)


def yellow(t):
    return paint("1;93", t)


def request(args, path, body=None):
    req = urllib.request.Request(
        args.url + path,
        data=body.encode() if body is not None else None,
        headers={"x-funnel-tenant": args.tenant},
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read().decode()
            return data, dict(resp.headers), (time.monotonic() - start) * 1000
    except urllib.error.HTTPError as e:
        sys.exit(f"funnel: {e.code} {e.read().decode().strip()}")
    except urllib.error.URLError as e:
        sys.exit(f"funnel: cannot reach {args.url} ({e.reason})")


def when(text):
    if text.isdigit():
        return int(text)
    hour, minute = map(int, text.split(":"))
    today = dt.datetime.now(dt.timezone.utc).replace(hour=hour, minute=minute, second=0, microsecond=0)
    return int(today.timestamp())


def number(v):
    if v is None:
        return "-"
    if isinstance(v, float) and not v.is_integer():
        return f"{v:,.1f}"
    if isinstance(v, (int, float)):
        return f"{v:,.0f}"
    return str(v)


def visible(text):
    return len(re.sub(r"\033\[[0-9;]*m", "", str(text)))


def table(header, rows, right=None):
    right = right or set()
    widths = [max(visible(h), *(visible(r[i]) for r in rows)) if rows else visible(h)
              for i, h in enumerate(header)]

    def line(cells, style=lambda s: s):
        out = []
        for i, c in enumerate(cells):
            pad = " " * (widths[i] - visible(c))
            out.append(style(pad + str(c) if i in right else str(c) + pad))
        return "  ".join(out).rstrip()

    print(line(header, dim))
    for r in rows:
        print(line(r))


def cmd_query(args):
    params = {"metric": args.metric, "agg": args.agg, "step": args.step}
    if args.by:
        params["group_by"] = args.by
    if args.where:
        params["where"] = args.where
    if args.since:
        params["from"] = when(args.since)
    if args.until:
        params["to"] = when(args.until)
    body, _, ms = request(args, "/query?" + urllib.parse.urlencode(params))
    data = json.loads(body)
    series = data["series"]
    if not series:
        print(dim("no data in that range"))
        return
    names = [",".join(f"{v}" for v in s["tags"].values()) or "all" for s in series]
    times = sorted({p[0] for s in series for p in s["points"]})
    values = [{p[0]: p[1] for p in s["points"]} for s in series]

    print(bold(f"{args.agg} of {args.metric}") + dim(f"  every {args.step // 60} min, tenant {args.tenant}, UTC"))
    everything = [x for v in values for x in v.values() if x is not None]
    # Flag values far above typical, so an outlier stands out.
    typical = statistics.median(everything)
    rows = []
    for t in times:
        row = [time.strftime("%H:%M", time.gmtime(t))]
        for v in values:
            c = v.get(t)
            text = number(c)
            row.append(yellow(text + " !") if c is not None and c > 3 * typical else text + "  ")
        rows.append(row)
    table(["time", *names], rows, right=set(range(1, len(names) + 1)))
    print(dim(f"\n{data['buckets_read']:,} summaries read from {data['files_read']} files in {ms:.0f} ms"))


STAMP = re.compile(r"^(\d{4}-\d\d-\d\d)T(\d\d:\d\d):\d\d")


def short_times(rows):
    """Shows timestamps as HH:MM when they all fall on one day, and returns that day."""
    days = {m.group(1) for r in rows for v in r.values() if isinstance(v, str) and (m := STAMP.match(v))}
    if len(days) != 1:
        return rows, None
    out = [{k: (STAMP.match(v).group(2) if isinstance(v, str) and STAMP.match(v) else v)
            for k, v in r.items()} for r in rows]
    return out, days.pop()


TABLE_NAMES = {10: "10-second", 60: "1-minute", 600: "10-minute", 3600: "1-hour"}


def cmd_sql(args):
    sql = " ".join(args.sql)
    body, headers, ms = request(args, "/sql", sql)
    rows, day = short_times(json.loads(body))
    if day:
        print(dim(f"times are UTC on {day}"))
    if rows:
        header = list(rows[0].keys())
        numeric = {i for i, h in enumerate(header) if isinstance(rows[0][h], (int, float))}
        table(header, [[number(r.get(h)) for h in header] for r in rows], right=numeric)
    else:
        print(dim("no rows"))
    seconds = headers.get("x-funnel-rollup-seconds") or headers.get("X-Funnel-Rollup-Seconds")
    where = (green(f"answered from the {TABLE_NAMES.get(int(seconds), seconds + '-second')} table")
             if seconds else "read the full 10-second table")
    print(f"\n{where}" + dim(f"  {len(rows)} rows in {ms:.0f} ms"))


def cmd_stats(args):
    body, _, _ = request(args, "/stats")
    s = json.loads(body)
    c = s["counters"]
    rejected = sum(c[k] for k in c if k.startswith("rejected_"))
    table(
        ["", "count"],
        [
            ["lines received", number(c["lines_received"])],
            ["kept", number(c["lines_accepted"])],
            ["malformed", number(c["parse_errors"])],
            ["dropped (overload)", number(c["dropped_overload"])],
            ["rejected (limits, too old)", number(rejected)],
            ["10-second summaries written", number(c["buckets_flushed"])],
        ],
        right={1},
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("FUNNEL_URL", "http://127.0.0.1:8080"))
    ap.add_argument("--tenant", default=os.environ.get("FUNNEL_TENANT", "acme"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("query", help="one metric over time, per tag")
    q.add_argument("metric")
    q.add_argument("--agg", default="sum", help="sum, rate, min, max, avg, last, count, p50, p99 ...")
    q.add_argument("--by", help="tag to split by, such as host")
    q.add_argument("--where", help="tag filter, such as route:/checkout")
    q.add_argument("--step", type=int, default=60, help="seconds per row")
    q.add_argument("--from", dest="since", help="HH:MM UTC today, or unix seconds")
    q.add_argument("--to", dest="until", help="HH:MM UTC today, or unix seconds")
    q.set_defaults(fn=cmd_query)

    s = sub.add_parser("sql", help="a read-only SQL query over the Iceberg tables")
    s.add_argument("sql", nargs="+")
    s.set_defaults(fn=cmd_sql)

    st = sub.add_parser("stats", help="what Funnel has received and kept")
    st.set_defaults(fn=cmd_stats)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
