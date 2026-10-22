"""End-to-end check of query freshness, crash loss and compaction.

    cargo build --release -p funnel
    python3 scripts/durability_test.py [data dir] [extra funnel flags...]

Starts the real server, sends 100 counter lines a second, and measures how
long a new point takes to appear in queries, how much is lost to kill -9
(the server is restarted on the same data), and that compaction merges
files without changing query totals. Uses ports 18125 and 18080.
"""
import json, os, signal, socket, subprocess, sys, tempfile, threading, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release", "funnel")
DATA = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="funnel-durability-")
UDP, HTTP = ("127.0.0.1", 18125), "http://127.0.0.1:18080"
ARGS = [BIN, "--udp", "127.0.0.1:18125", "--http", "127.0.0.1:18080", "--shards", "2",
        "--data-dir", DATA, "--span-secs", "20", "--settle-secs", "20"] + sys.argv[2:]

def start():
    p = subprocess.Popen(ARGS, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            urllib.request.urlopen(HTTP + "/healthz", timeout=1)
            return p
        except OSError:
            time.sleep(0.1)
    raise SystemExit("funnel did not start")

def query(params):
    now = int(time.time())
    url = f"{HTTP}/query?from={now - 3600}&to={now + 60}&" + params
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)

def total(metric):
    d = query(f"metric={metric}&group_by=&step=7200&agg=sum")
    return sum(v for s in d["series"] for _, v in s["points"])

def files():
    return sum(len([f for f in fs if f.endswith(".fnlb.zst")]) for _, _, fs in os.walk(DATA))

def size():
    return sum(os.path.getsize(os.path.join(d, f))
               for d, _, fs in os.walk(DATA) for f in fs if f.endswith(".fnlb.zst"))

sent = 0
stop = False
def load():
    """100 counter increments per second, in packets of 10."""
    global sent
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while not stop:
        s.sendto("\n".join(["req:1|c"] * 10).encode(), UDP)
        sent += 10
        time.sleep(0.1)

p = start()
threading.Thread(target=load, daemon=True).start()

# 1. Freshness: how long until a uniquely tagged point is queryable.
time.sleep(3)
lat = []
for i in range(3):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t0 = time.time()
    s.sendto(f"marker:1|c|#id:m{i}".encode(), UDP)
    while not query(f"metric=marker&where=id:m{i}")["series"]:
        time.sleep(0.2)
    lat.append(time.time() - t0)
print(f"freshness: visible after {', '.join(f'{x:.1f}s' for x in lat)}")

# 2. Crash: kill -9 mid-load, restart, compare what survived to what was sent.
time.sleep(12)
sent_at_kill = sent
p.send_signal(signal.SIGKILL)
p.wait()
p = start()
survived = total("req")
print(f"crash: sent {sent_at_kill} before kill -9, {survived:.0f} survived "
      f"({sent_at_kill - survived:.0f} lost, about {(sent_at_kill - survived) / 100:.1f}s of traffic)")

# 3. Compaction: keep loading, then wait for passes and compare totals.
time.sleep(20)
stop = True
time.sleep(25)  # let the last windows close and be written
before_files, before_total = files(), total("req")
print(f"disk: {size()} bytes before compaction")
deadline = time.time() + 150
while files() >= before_files and time.time() < deadline:
    time.sleep(5)
time.sleep(70)  # a couple more passes, and trash deletion
after_files, after_total = files(), total("req")
print(f"disk: {size()} bytes after compaction")
print(f"compaction: {before_files} files -> {after_files}, "
      f"total {before_total:.0f} -> {after_total:.0f}")
stats = json.load(urllib.request.urlopen(HTTP + "/stats"))["counters"]
print("stats:", {k: v for k, v in stats.items() if k in
      ("files_compacted", "blocks_written", "buckets_lost_write", "lines_accepted")})
p.send_signal(signal.SIGINT)
p.wait()
