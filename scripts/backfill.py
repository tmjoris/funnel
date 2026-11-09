#!/usr/bin/env python3
"""Sends a few hours of made-up service metrics to a running Funnel.

Timestamps are in the past so that whole hours are already closed and can be
published to the lake within about a minute.
"""
import argparse
import random
import time
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--url", default="http://127.0.0.1:8080")
parser.add_argument("--tenant", default="acme")
parser.add_argument("--hours", type=int, default=3)
parser.add_argument("--end", type=int, help="unix time the data ends at, on an hour boundary")
args = parser.parse_args()

HOUR = 3600
now = int(time.time())
# Stop at the last hour that closed at least 15 minutes ago, so it can be published right away.
end = args.end if args.end else (now - 900) // HOUR * HOUR
start = end - args.hours * HOUR
rng = random.Random(7)


def send(lines):
    body = "\n".join(lines).encode()
    request = urllib.request.Request(args.url + "/ingest", data=body, headers={"x-funnel-tenant": args.tenant})
    urllib.request.urlopen(request).read()


batch, total = [], 0
for ts in range(start, end, 10):
    for host in ("web-1", "web-2", "web-3"):
        batch.append(f"http.requests:{rng.randint(50, 150)}|c|#host:{host}|T{ts}")
        batch.append(f"queue.depth:{rng.randint(0, 40)}|g|#host:{host}|T{ts}")
        for _ in range(5):
            slow = 30 if rng.random() < 0.02 else 1
            batch.append(f"http.duration:{rng.uniform(5, 40) * slow:.2f}|ms|#host:{host}|T{ts}")
    if len(batch) >= 2000:
        send(batch)
        total += len(batch)
        batch = []
if batch:
    send(batch)
    total += len(batch)
print(f"sent {total} points covering {time.strftime('%H:%M', time.gmtime(start))} to {time.strftime('%H:%M', time.gmtime(end))} UTC")
