#!/usr/bin/env python3
"""Measures Funnel's ingest throughput at increasing UDP rates.

    cargo build --release -p funnel -p loadgen
    python3 scripts/loadtest.py [--rates 50000,100000,0] [--duration 15]
                                [--shards 2] [--udp-threads 2] [--senders 2]

For each target rate (0 = as fast as the sender can go) it starts a fresh
server, replays the generated workload with loadgen and reports what was
sent, what Funnel accepted, what Funnel shed on purpose (its queues were
full), what the kernel dropped before Funnel could read it (UDP receive
buffer full), the CPU Funnel used and its peak resident memory.
Linux only: it reads /proc.
"""
import argparse, json, os, shutil, subprocess, tempfile, threading, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUNNEL = os.path.join(ROOT, "target", "release", "funnel")
LOADGEN = os.path.join(ROOT, "target", "release", "loadgen")
HTTP = "http://127.0.0.1:18080"


def udp_snmp():
    """System-wide UDP counters: datagrams received and receive-buffer drops."""
    with open("/proc/net/snmp") as f:
        rows = [l.split() for l in f if l.startswith("Udp:")]
    stats = dict(zip(rows[0][1:], map(int, rows[1][1:])))
    return stats["InDatagrams"], stats["RcvbufErrors"]


def cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return (int(fields[11]) + int(fields[12])) / ticks  # utime + stime


def rss_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def counters():
    with urllib.request.urlopen(HTTP + "/stats", timeout=5) as r:
        return json.load(r)["counters"]


def run(rate, args):
    data = tempfile.mkdtemp(prefix="funnel-load-")
    pin = ["taskset", "-c", args.cpus] if args.cpus else []
    server = subprocess.Popen(
        pin + [args.funnel, "--udp", "127.0.0.1:18125", "--http", "127.0.0.1:18080",
         "--shards", str(args.shards), "--udp-threads", str(args.udp_threads),
         "--data-dir", data],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                counters()
                break
            except OSError:
                time.sleep(0.05)
        before, snmp0, cpu0 = counters(), udp_snmp(), cpu_seconds(server.pid)
        peak = [0.0]
        done = threading.Event()

        def sample():
            while not done.is_set():
                peak[0] = max(peak[0], rss_mb(server.pid))
                time.sleep(0.2)

        threading.Thread(target=sample, daemon=True).start()
        t0 = time.time()
        out = subprocess.run(
            [LOADGEN, "--udp", "127.0.0.1:18125", "--rate", str(rate),
             "--duration", str(args.duration), "--threads", str(args.senders),
             "--packet-bytes", str(args.packet_bytes)],
            capture_output=True, text=True, check=True).stdout
        # Let queued batches drain before reading counters.
        time.sleep(2)
        elapsed = time.time() - t0
        done.set()
        sent = json.loads(out)
        after, snmp1, cpu1 = counters(), udp_snmp(), cpu_seconds(server.pid)
        d = {k: after[k] - before[k] for k in after}
        secs = sent["seconds"]
        return {
            "target": rate,
            "sent_per_s": sent["lines_sent"] / secs,
            "accepted_per_s": d["lines_accepted"] / secs,
            "shed_per_s": d["dropped_overload"] / secs,
            "lines_received": d["lines_received"],
            "lines_sent": sent["lines_sent"],
            "kernel_drops": snmp1[1] - snmp0[1],
            "datagrams": snmp1[0] - snmp0[0],
            "cpu_cores": (cpu1 - cpu0) / elapsed,
            "rss_mb": peak[0],
            "series_limited": d["rejected_cardinality"],
        }
    finally:
        server.terminate()
        server.wait()
        shutil.rmtree(data, ignore_errors=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rates", default="50000,100000,200000,400000,0")
    p.add_argument("--duration", type=int, default=15)
    p.add_argument("--shards", type=int, default=2)
    p.add_argument("--udp-threads", type=int, default=2)
    p.add_argument("--senders", type=int, default=2)
    p.add_argument("--funnel", default=FUNNEL, help="server binary, to compare builds")
    p.add_argument("--packet-bytes", type=int, default=1432,
                   help="largest UDP payload loadgen sends; small values model one metric per packet")
    p.add_argument("--cpus", default="",
                   help="pin the server to these CPUs with taskset, e.g. 0 to model a 1-vCPU server")
    args = p.parse_args()
    print(f"{os.cpu_count()} CPUs, server pinned to {args.cpus or 'all'}; shards={args.shards} "
          f"udp_threads={args.udp_threads} senders={args.senders}; {args.duration}s per rate")
    print(f"{'target/s':>9} {'sent/s':>10} {'accepted/s':>11} {'shed/s':>9} "
          f"{'lost in kernel':>15} {'CPU cores':>9} {'RSS MB':>7}")
    for rate in [int(r) for r in args.rates.split(",")]:
        r = run(rate, args)
        lost = r["lines_sent"] - r["lines_received"]
        print(f"{r['target'] or 'max':>9} {r['sent_per_s']:>10.0f} {r['accepted_per_s']:>11.0f} "
              f"{r['shed_per_s']:>9.0f} {lost:>9} lines {r['cpu_cores']:>9.2f} {r['rss_mb']:>7.0f}"
              + (f"  ({r['kernel_drops']} buffer drops)" if r["kernel_drops"] else ""))


if __name__ == "__main__":
    main()
