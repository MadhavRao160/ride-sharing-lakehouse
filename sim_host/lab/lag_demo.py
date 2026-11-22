#!/usr/bin/env python3
"""Lab 9: consumer lag.
A producer writes IN msg/s to topic lab_lag (6 partitions). CONSUMERS consumers in one group
each read at SLOW msg/s for SLOW_S seconds, then at FAST msg/s (rates are PER consumer).
A monitor prints, every few seconds, per partition:
  lag = newest offset - committed offset (messages)
  age = now - timestamp of the last message read from that partition (lag in seconds)
It stops when every partition has caught up (age < 2 s) or after FAST_MAX_S in the fast phase.
skew>0 sends that share of messages straight to partition 0 (a hot key).
Usage: lag_demo.py [in_rate=2000] [slow=1500] [fast=3000] [slow_s=60] [skew=0]
                   [consumers=1] [fast_max_s=180]"""
import multiprocessing as mp
import os
import random
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

TOPIC = "lab_lag"
PARTS = 6


def ensure_topic():
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient(kafka_conf())
    if TOPIC in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics([NewTopic(TOPIC, PARTS, 3, config={
        "retention.ms": "3600000", "min.insync.replicas": "2"})])
    fut[TOPIC].result()
    print(f"created topic {TOPIC} ({PARTS} partitions, RF 3, retention 1h)", flush=True)
    time.sleep(2)


def producer(rate, skew, go, stop):
    from confluent_kafka import Producer
    p = Producer(kafka_conf(**{"linger.ms": 5, "acks": "all"}))
    go.wait()
    t0, sent, value = time.time(), 0, b"x" * 60
    while not stop.is_set():
        due = int((time.time() - t0) * rate)
        while sent < due:
            key = str(random.randrange(100000)).encode()
            if skew and random.random() < skew:
                p.produce(TOPIC, value, key, partition=0)
            else:
                p.produce(TOPIC, value, key)
            sent += 1
        p.poll(0)
        time.sleep(0.005)
    p.flush(10)


def consumer(i, group, slow, fast, assigned, stop, switch, last_ts):
    from confluent_kafka import Consumer
    c = Consumer(kafka_conf(**{"group.id": group, "auto.offset.reset": "latest",
                               "enable.auto.commit": True, "auto.commit.interval.ms": 1000}))
    def on_assign(_c, parts):
        assigned[i] = 1 if parts else 0

    def on_revoke(_c, _parts):
        assigned[i] = 0

    c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    allowance, last = 0.0, time.time()
    while not stop.is_set():                      # token bucket: read at most `rate` msg/s
        now = time.time()
        rate = fast if switch.is_set() else slow
        allowance = min(allowance + rate * (now - last), rate * 0.2)
        last = now
        n = int(allowance)
        if n < 1:
            time.sleep(0.002)
            continue
        for m in c.consume(num_messages=min(n, 500), timeout=0.1):
            if m.error():
                continue
            allowance -= 1
            last_ts[m.partition()] = m.timestamp()[1] / 1000
    c.close()


def main():
    a = sys.argv[1:]
    in_rate = float(a[0]) if len(a) > 0 else 2000
    slow = float(a[1]) if len(a) > 1 else 1500
    fast = float(a[2]) if len(a) > 2 else 3000
    slow_s = float(a[3]) if len(a) > 3 else 60
    skew = float(a[4]) if len(a) > 4 else 0.0
    n_cons = int(a[5]) if len(a) > 5 else 1
    fast_max_s = float(a[6]) if len(a) > 6 else 180

    from confluent_kafka import Consumer, TopicPartition
    ensure_topic()
    group = f"lag-lab-{int(time.time())}"
    go, stop, switch = mp.Event(), mp.Event(), mp.Event()
    last_ts = mp.Array("d", PARTS)
    assigned = mp.Array("i", n_cons)
    cps = [mp.Process(target=consumer, args=(i, group, slow, fast, assigned, stop, switch, last_ts))
           for i in range(n_cons)]
    pp = mp.Process(target=producer, args=(in_rate, skew, go, stop))
    procs = cps + [pp]
    for p in procs:
        p.start()
    want = min(n_cons, PARTS)                      # consumers beyond 6 sit idle
    deadline = time.time() + 60
    while sum(assigned[:]) < want and time.time() < deadline:
        time.sleep(0.5)
    if sum(assigned[:]) < want:
        print("consumers did not all get partitions in 60 s; is the cluster up?")
        stop.set(); go.set()
        for p in procs:
            p.join()
        return
    time.sleep(5)                                  # let the group settle at the newest offset

    mon = Consumer(kafka_conf(**{"group.id": group, "enable.auto.commit": False}))
    tps = [TopicPartition(TOPIC, p) for p in range(PARTS)]
    base = {p: mon.get_watermark_offsets(tps[p], timeout=5, cached=False)[1] for p in range(PARTS)}

    def lag_now():
        com = {tp.partition: tp.offset for tp in mon.committed(tps, timeout=10)}
        out = {}
        for p in range(PARTS):
            high = mon.get_watermark_offsets(tps[p], timeout=5, cached=False)[1]
            pos = com[p] if com[p] >= 0 else base[p]
            out[p] = max(0, high - pos)
        return out

    print(f"group={group} in={in_rate:g}/s, {n_cons} consumer(s) each slow={slow:g}/s for {slow_s:g}s, "
          f"then fast={fast:g}/s{f', skew={skew:g} to p0' if skew else ''}", flush=True)
    print(f"{'t':>5}  {'phase':5}  {'total lag':>9}  {'worst age':>9}  per partition: lag msgs (age s)")
    go.set()
    t0 = time.time()
    switched_at, caught, backlog, predicted = None, None, 0, None
    while True:
        time.sleep(2 if switched_at else 5)
        lag = lag_now()
        now = time.time()
        total = sum(lag.values())
        ages = {p: (now - last_ts[p]) if last_ts[p] else float("nan") for p in range(PARTS)}
        wa = max(ages, key=lambda p: ages[p] if ages[p] == ages[p] else 1e9)
        age = ages[wa]
        phase = "FAST" if switch.is_set() else "slow"
        print(f"{now - t0:5.0f}  {phase:5}  {total:9,}  {f'p{wa}:{age:.0f}s':>9}  "
              + "  ".join(f"p{p} {lag[p]:,}({ages[p]:.0f})" for p in range(PARTS)), flush=True)
        if switched_at is None and now - t0 >= slow_s:
            switch.set()
            switched_at, backlog = time.time(), total
            if n_cons == 1:
                predicted = backlog / (fast - in_rate) if fast > in_rate else float("inf")
                print(f"-- switch to {fast:g}/s. backlog {backlog:,} msgs. "
                      f"predicted catch-up = {backlog:,} / ({fast:g} - {in_rate:g}) = {predicted:.0f}s",
                      flush=True)
            else:
                print(f"-- switch: each consumer now reads up to {fast:g}/s. backlog {backlog:,} msgs",
                      flush=True)
        elif switched_at and age < 2.0:
            caught = time.time() - switched_at
            break
        elif switched_at and time.time() - switched_at > fast_max_s:
            break

    stop.set()
    for p in procs:
        p.join()
    mon.close()
    print("\n== result")
    print(f"backlog at switch : {backlog:,} msgs")
    if predicted is not None:
        print(f"predicted catch-up: {predicted:.0f}s")
    print(f"measured catch-up : "
          f"{f'{caught:.0f}s (2 s resolution)' if caught else f'not caught up in {fast_max_s:g} s'}")
    print("final lag per partition: " + "  ".join(f"p{p} {lag[p]:,}" for p in range(PARTS)))


if __name__ == "__main__":
    main()
