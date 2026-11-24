#!/usr/bin/env python3
"""Lab 16: hot partition (interactive, multiple-choice predictions).
Topic: 6 partitions. 1,000 normal drivers send 500 msg/s in total (about 83 per partition), key = driver_id.
Each consumer can process at most CAP msg/s (simulated work per message).
  1. normal traffic: every partition keeps up
  2. driver 'd-bug' starts sending 500 msg/s alone (1000x a normal driver): its partition overloads
  3. scale consumers 6 -> 12: does the hot partition catch up?
  4. fix A, cap at the source: d-bug limited to 5 msg/s, the rest diverted to a quarantine topic
  5. fix B, salting: d-bug at full speed again but its key spread over 6 partitions. What does it cost?
Usage: ./local/lab.sh hot_demo.py"""
import json
import os
import random
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

TOPIC = f"lab_hot_{int(time.time()) % 100000}"
QTOPIC = TOPIC + "_quarantine"
PARTS, CAP = 6, 200                 # partitions; max msg/s one consumer can process
NORMAL_RATE, BUG_RATE, BUG_LIMIT = 500, 500, 5
GROUP = f"hot-{uuid.uuid4().hex[:8]}"
log = []
lock = threading.Lock()
S = {"mode": "normal", "stop": False, "frozen": False, "hot": None, "pos": {}, "ts": {},
     "salted_last": -1, "salted_seen": 0, "inversions": 0, "quarantined": 0}


def line(t=""):
    print(t, flush=True)


def freeze(on):
    """Stop producing and consuming while Madhav reads or answers, so the backlog does not change."""
    S["frozen"] = on
    if on:
        time.sleep(0.3)


def wait_enter():
    freeze(True)
    input("   Press Enter to continue... ")
    freeze(False)


def ask(q, options, correct):
    freeze(True)
    line(f"\n?? PREDICT: {q}")
    for k, v in options.items():
        line(f"     {k}) {v}")
    a = input("   Your choice (letter): ").strip().lower()
    line()
    log.append((q, a, correct, options[correct]))
    freeze(False)


# ---------------------------------------------------------------- producer
def on_delivery(err, msg):
    if err is None and msg.key() == b"d-bug" and S["hot"] is None:
        S["hot"] = msg.partition()


def producer_loop():
    p = Producer(kafka_conf(**{"acks": "all", "linger.ms": 5}))
    seq, t0, sent_n = 0, time.time(), 0
    mode, b0, sent_b = None, time.time(), 0
    while not S["stop"]:
        if S["frozen"]:
            f0 = time.time()
            while S["frozen"] and not S["stop"]:
                p.poll(0.1)
            t0 += time.time() - f0
            b0 += time.time() - f0
            continue
        now = time.time()
        while sent_n < (now - t0) * NORMAL_RATE:
            d = f"d-{random.randint(0, 999):04d}"
            p.produce(TOPIC, json.dumps({"d": d, "ts": time.time()}).encode(), key=d)
            sent_n += 1
        if S["mode"] != mode:               # new phase: restart the bug driver's clock
            mode, b0, sent_b = S["mode"], now, 0
        bug_rate = 0 if mode == "normal" else BUG_RATE
        while sent_b < (now - b0) * bug_rate:
            v = {"d": "d-bug", "seq": seq, "ts": time.time()}
            if mode == "limit" and seq % (BUG_RATE // BUG_LIMIT) != 0:
                p.produce(QTOPIC, json.dumps(v).encode(), key="d-bug")       # diverted, not processed
                S["quarantined"] += 1
            elif mode == "salted":
                v["salted"] = 1
                p.produce(TOPIC, json.dumps(v).encode(), key=f"d-bug#{seq % PARTS}", partition=seq % PARTS)
            else:
                p.produce(TOPIC, json.dumps(v).encode(), key="d-bug", on_delivery=on_delivery)
            seq += 1
            sent_b += 1
        p.poll(0)
        time.sleep(0.01)
    p.flush(10)


# ---------------------------------------------------------------- consumers
def consumer_loop(name):
    c = Consumer(kafka_conf(**{"group.id": GROUP, "auto.offset.reset": "earliest",
                               "enable.auto.commit": True, "auto.commit.interval.ms": 1000,
                               "max.poll.interval.ms": 3600000}))
    c.subscribe([TOPIC])
    next_free = time.time()
    while not S["stop"]:
        if S["frozen"]:
            time.sleep(0.1)
            next_free = time.time()
            continue
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        v = json.loads(m.value())
        with lock:
            S["pos"][m.partition()] = m.offset() + 1
            S["ts"][m.partition()] = v["ts"]
            if v.get("salted"):
                S["salted_seen"] += 1
                if v["seq"] < S["salted_last"]:
                    S["inversions"] += 1
                S["salted_last"] = max(S["salted_last"], v["seq"])
        # simulated work: at most CAP msg/s per consumer, no saved-up credit from idle time
        next_free = max(next_free, time.time()) + 1.0 / CAP
        if next_free > time.time():
            time.sleep(next_free - time.time())
    c.close()


def start_consumers(n, first):
    for i in range(n):
        threading.Thread(target=consumer_loop, args=(f"c{first + i}",), daemon=True).start()


# ---------------------------------------------------------------- lag view
wm = None


def lag_rows():
    rows = []
    now = time.time()
    for p in range(PARTS):
        _, high = wm.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=2, cached=False)
        with lock:
            pos, ts = S["pos"].get(p, 0), S["ts"].get(p, now)
        msgs = max(high - pos, 0)
        rows.append((p, msgs, (now - ts) if msgs > 0 else 0.0))
    return rows


def status(label, seconds, until=None):
    t0 = time.time()
    while time.time() - t0 < seconds:
        time.sleep(3)
        rows = lag_rows()
        total = sum(r[1] for r in rows)
        cells = "  ".join(f"p{p}{'*' if p == S['hot'] else ' '}{s:5.0f}s" for p, _, s in rows)
        hot = next((r for r in rows if r[0] == S["hot"]), None)
        hot_txt = f"hot backlog {hot[1]:>6,}" if hot else ""
        line(f"   [{label:<9} t+{time.time() - t0:4.0f}s] lag in seconds: {cells} | total msgs {total:>6,} {hot_txt}")
        if until and until(rows):
            return time.time() - t0
    return None


def main():
    global wm
    admin = AdminClient(kafka_conf())
    admin.create_topics([NewTopic(TOPIC, PARTS, 3), NewTopic(QTOPIC, 1, 3)])
    time.sleep(3)
    wm = Consumer(kafka_conf(**{"group.id": f"wm-{uuid.uuid4().hex[:6]}"}))
    line(f"== Lab 16: hot partition. Topic {TOPIC}: {PARTS} partitions, key = driver_id")
    line(f"   1,000 normal drivers send {NORMAL_RATE} msg/s in total (about {NORMAL_RATE // PARTS} per partition).")
    line(f"   Each consumer can process at most {CAP} msg/s. Status: lag in SECONDS per partition (* = hot one).")

    threading.Thread(target=producer_loop, daemon=True).start()
    start_consumers(PARTS, 1)

    line("\n== Phase 1: normal traffic, 6 consumers, 20 s")
    status("normal", 20)

    ask(f"Driver 'd-bug' starts sending {BUG_RATE} msg/s alone (1000x a normal driver). What happens?",
        {"a": "all 6 partitions slow down a little",
         "b": "one partition falls further behind every second; the other five are fine",
         "c": "Kafka notices and throttles that driver"}, "b")
    S["mode"] = "bug"
    line("\n== Phase 2: d-bug on, 15 s")
    status("bug on", 15)
    line(f"   WHY: same key -> same partition (p{S['hot']}). It now gets about {NORMAL_RATE // PARTS + BUG_RATE} msg/s,")
    line(f"   but its ONE consumer handles {CAP} msg/s, so the backlog grows by about "
         f"{NORMAL_RATE // PARTS + BUG_RATE - CAP} msg every second.")
    line("   Kafka itself is fine (writes are cheap); the READER is the bottleneck. Every normal driver who")
    line(f"   shares p{S['hot']} with d-bug is now late too: about 1 in 6 of all drivers.")
    line(f"   Optional, in Kafka UI: Consumers -> {GROUP} -> lag per partition")
    wait_enter()

    ask("We double the consumers from 6 to 12. Does the hot partition catch up?",
        {"a": "yes, about twice as fast",
         "b": "no: a partition is read by only ONE consumer in a group, so 6 of the 12 sit idle",
         "c": "partly: the other partitions' consumers help out"}, "b")
    start_consumers(PARTS, PARTS + 1)
    line("\n== Phase 3: 12 consumers, 15 s")
    status("12 cons", 15)
    line("   WHY: max useful consumers in a group = number of partitions (Lab 5). The 6 new ones get no")
    line("   partition. Adding partitions would not help either: d-bug's one key still lands on one partition.")
    wait_enter()

    rows = lag_rows()
    hot = next(r for r in rows if r[0] == S["hot"])
    incoming = NORMAL_RATE // PARTS + BUG_LIMIT
    ask(f"Fix A: cap d-bug at {BUG_LIMIT} msg/s at the source and divert the rest to a quarantine topic. "
        f"The hot backlog is about {hot[1]:,} messages, the reader does {CAP}/s, about {incoming}/s keep arriving. "
        "Roughly how long to catch up?",
        {"a": f"about {hot[1] // CAP} s (backlog / read speed)",
         "b": f"about {hot[1] // (CAP - incoming)} s (backlog / (read speed - incoming))",
         "c": "never, the partition stays behind"}, "b")
    S["mode"] = "limit"
    line("\n== Phase 4: d-bug capped, waiting until the hot partition is less than 3 s behind (max 240 s)")
    took = status("capped", 240, until=lambda rs: all(s < 3 for _, _, s in rs))
    line(f"   caught up after {took:.0f} s" if took else "   not fully caught up in 240 s")
    line(f"   quarantined messages (kept, not processed): {S['quarantined']:,}")
    line("   WHY: the extra read speed (read - incoming) pays off the backlog, the Lab 9 formula. The")
    line("   quarantine topic keeps the bad data for later checks instead of silently dropping it.")
    wait_enter()

    ask("Fix B instead: d-bug at full speed again, but its key is SALTED (d-bug#0 ... d-bug#5) so it spreads "
        "over all 6 partitions. What do we lose?",
        {"a": "nothing, this is the best fix",
         "b": "per-driver ORDER: d-bug's pings are now read in parallel, out of order",
         "c": "durability: some pings are lost"}, "b")
    with lock:
        S["inversions"], S["salted_seen"], S["salted_last"] = 0, 0, -1
    S["mode"] = "salted"
    line("\n== Phase 5: d-bug salted over 6 partitions, 25 s")
    status("salted", 25)
    with lock:
        inv, seen = S["inversions"], S["salted_seen"]
    line(f"   d-bug pings processed: {seen:,} | processed OUT OF ORDER: {inv:,} "
         f"({100 * inv / max(seen, 1):.0f}%)")
    line(f"   WHY: each partition now gets about {NORMAL_RATE // PARTS + BUG_RATE // PARTS} msg/s, under the {CAP}/s")
    line("   limit, so no partition falls behind. But one driver's pings are in 6 partitions read in parallel,")
    line("   so a later ping can be processed before an earlier one. For GPS that means a driver's position")
    line("   can 'jump back'. Salting is fine for counts and sums, wrong when order per key matters.")

    S["stop"] = True
    time.sleep(2)
    line("\n== Your predictions vs results")
    score = 0
    for q, mine, correct, ans in log:
        ok = mine == correct
        score += ok
        line(f"   {'RIGHT' if ok else 'wrong'}  you: {mine or '-'}  answer: {correct}) {ans[:50]:<50}  {q[:40]}")
    line(f"   score: {score}/{len(log)}")
    line("\n== Answer these 4 in chat")
    line("   SDE2:      what is a hot partition, and how do you spot one?")
    line("   Principal: lag grows on one partition. Why don't more consumers fix it, and what does?")
    line("   Chief Architect: driver_id is our key for gps_pings. How do you protect the platform from one")
    line("              bad producer without breaking per-driver ordering?")
    line("   Senior Director: a firmware bug made 2% of phones send 100x pings for an hour; surge was 15 min")
    line("              behind in two cities. Walk me through it (5 parts).")


if __name__ == "__main__":
    main()
