#!/usr/bin/env python3
"""Lab 5: consumer groups.
Starts N consumers in ONE group, one every `stagger` seconds, and prints each time
partitions are assigned to or taken from a consumer (a rebalance).
At the end: messages read per consumer and per partition.
New groups start at the newest offset, so lag starts near zero.
Usage: group_demo.py <group> <consumers> [run_seconds=60] [stagger_s=8] [topic=gps_pings]"""
import multiprocessing as mp
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

group = sys.argv[1]
n = int(sys.argv[2])
run_s = float(sys.argv[3]) if len(sys.argv) > 3 else 60
stagger = float(sys.argv[4]) if len(sys.argv) > 4 else 8
topic = sys.argv[5] if len(sys.argv) > 5 else "gps_pings"


def worker(i, t0, end_at, q):
    from confluent_kafka import Consumer
    time.sleep(max(0.0, t0 + i * stagger - time.time()))
    name = f"c{i + 1}"

    def log(msg):
        print(f"[t+{time.time() - t0:5.1f}s] {name} {msg}", flush=True)

    def on_assign(c, parts):
        log(f"ASSIGNED {sorted(p.partition for p in parts) or 'nothing (idle)'}")

    def on_revoke(c, parts):
        log(f"revoked  {sorted(p.partition for p in parts)}")

    c = Consumer(kafka_conf(**{"group.id": group, "client.id": f"{group}-{name}",
                               "auto.offset.reset": "latest", "enable.auto.commit": True}))
    c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    log("joined the group")
    counts = Counter()
    while time.time() < end_at:
        m = c.poll(0.5)
        if m is not None and not m.error():
            counts[m.partition()] += 1
    c.close()
    q.put((name, dict(counts)))


if __name__ == "__main__":
    t0 = time.time() + 2
    end_at = t0 + (n - 1) * stagger + run_s
    print(f"group={group} consumers={n} topic={topic}: one joins every {stagger:g}s, "
          f"all run until t+{end_at - t0:.0f}s", flush=True)
    q = mp.Queue()
    procs = [mp.Process(target=worker, args=(i, t0, end_at, q)) for i in range(n)]
    for p in procs:
        p.start()
    results = dict(q.get() for _ in procs)
    for p in procs:
        p.join()
    print("\n== messages read per consumer (partition: count)")
    for name in sorted(results, key=lambda s: int(s[1:])):
        cnt = results[name]
        total = sum(cnt.values())
        detail = ", ".join(f"p{k}:{v}" for k, v in sorted(cnt.items())) or "none"
        print(f"{name}: {total:>8,}   {detail}")
