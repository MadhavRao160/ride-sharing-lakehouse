#!/usr/bin/env python3
"""Lab 4: producer acks 0 / 1 / all - speed vs durability.
Writes 70-byte messages to a test topic (lab_acks: 6 partitions, RF 3, min.insync 2)
and measures, for each acks setting:
  A. latency    - one message at a time, wait for the confirmation (p50 / p99 ms)
  B. throughput - N messages as fast as possible (msgs/s, p99 confirmation ms)
Idempotence is off in every run (it requires acks=all), so the comparison is fair.
Usage: acks_bench.py [latency_msgs=500] [throughput_msgs=100000]"""
import os
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

TOPIC = "lab_acks"
lat_n = int(sys.argv[1]) if len(sys.argv) > 1 else 500
thr_n = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
payload = os.urandom(70)

admin = AdminClient(kafka_conf())
if TOPIC not in admin.list_topics(timeout=15).topics:
    f = admin.create_topics([NewTopic(TOPIC, 6, 3, config={
        "min.insync.replicas": "2", "retention.ms": "3600000",
        "unclean.leader.election.enable": "false"})])
    f[TOPIC].result(30)
    time.sleep(5)
    print(f"created topic {TOPIC}")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else float("nan")


def run(acks):
    p = Producer(kafka_conf(**{
        "acks": acks, "enable.idempotence": False,
        "linger.ms": 5, "compression.type": "lz4",
        "partitioner": "murmur2_random",
    }))
    p.poll(1)
    p.produce(TOPIC, key=b"warmup", value=payload)
    p.flush(30)

    # A. latency: one at a time
    lat = []
    for i in range(lat_n):
        t0 = time.perf_counter()
        p.produce(TOPIC, key=str(i).encode(), value=payload)
        p.flush(30)
        lat.append((time.perf_counter() - t0) * 1000)

    # B. throughput: as fast as possible
    conf_ms, errors = [], [0]

    def cb(err, msg, t0):
        if err:
            errors[0] += 1
        else:
            conf_ms.append((time.perf_counter() - t0) * 1000)

    start = time.perf_counter()
    for i in range(thr_n):
        t0 = time.perf_counter()
        while True:
            try:
                p.produce(TOPIC, key=str(i).encode(), value=payload,
                          on_delivery=lambda e, m, t0=t0: cb(e, m, t0))
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
    p.flush(60)
    secs = time.perf_counter() - start
    return {
        "acks": acks,
        "lat_p50": pct(lat, 0.50), "lat_p99": pct(lat, 0.99),
        "msgs_s": thr_n / secs, "conf_p99": pct(conf_ms, 0.99), "errors": errors[0],
    }


results = [run(a) for a in ("0", "1", "all")]
print(f"\nmessages: latency test {lat_n} each, throughput test {thr_n} each, 70-byte values\n")
print(f"{'acks':<5} {'one-at-a-time p50 ms':>21} {'p99 ms':>8} {'throughput msgs/s':>18} {'confirm p99 ms':>15} {'errors':>7}")
for r in results:
    print(f"{r['acks']:<5} {r['lat_p50']:>21.2f} {r['lat_p99']:>8.2f} {r['msgs_s']:>18,.0f} {r['conf_p99']:>15.1f} {r['errors']:>7}")
base = results[1]
allr = results[2]
print(f"\nacks=all vs acks=1: latency p50 x{allr['lat_p50'] / base['lat_p50']:.2f}, "
      f"throughput x{allr['msgs_s'] / base['msgs_s']:.2f}")
