#!/usr/bin/env python3
"""Lab 17: partition sizing (interactive, multiple-choice predictions).
  1. write speed: 100,000 messages to a 1-partition topic vs a 6-partition topic (one producer)
  2. read speed: one consumer reads the 1-partition topic with no work, vs Lab 16's 200 msg/s with work
  3. sizing: 1,250 msg/s (pass-1 scale: 5,000 drivers every 4 s) on 6 vs 12 partitions, 200 msg/s per consumer
  4. add partitions to a LIVE keyed topic (6 -> 12): how many drivers move, and what happens to their order?
Usage: ./local/lab.sh partition_demo.py"""
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
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic  # noqa: E402

RUN = int(time.time()) % 100000
N_WRITE, MSG_BYTES = 100_000, 200
CAP = 200                       # max msg/s one consumer can process (simulated work, as in Lab 16)
TARGET = 1250                   # pass-1 scale: 5,000 drivers / 4 s
LIVE_RATE, LIVE_CAP, DRIVERS = 600, 80, 1000
# murmur2 = the Java client's default hash, so keys land where a Java producer would put them
PCONF = {"acks": "all", "linger.ms": 5, "partitioner": "murmur2_random",
         "topic.metadata.refresh.interval.ms": 2000}
log = []
S = {"frozen": False}
lock = threading.Lock()
admin = None
wm = None


def line(t=""):
    print(t, flush=True)


def freeze(on):
    """Pause producing and consuming while Madhav reads or answers, so the numbers do not move."""
    S["frozen"] = on
    if on:
        time.sleep(0.3)


def wait_enter():
    freeze(True)
    input("   Press Enter to continue... ")
    freeze(False)


def ask(q, options, correct=None):
    """correct=None means the answer depends on the measurement; set it later with settle()."""
    freeze(True)
    line(f"\n?? PREDICT: {q}")
    for k, v in options.items():
        line(f"     {k}) {v}")
    a = input("   Your choice (letter): ").strip().lower()
    line()
    log.append({"q": q, "mine": a, "correct": correct, "options": options})
    freeze(False)
    return len(log) - 1


def settle(i, correct):
    log[i]["correct"] = correct
    e = log[i]
    line(f"   -> answer: {correct}) {e['options'][correct]}   (you said {e['mine'] or '-'})")


def make_topic(name, parts):
    fs = admin.create_topics([NewTopic(name, parts, 3)])
    for f in fs.values():
        f.result()
    time.sleep(1)


def lag(topic, parts, pos):
    total = 0
    for p in range(parts):
        try:
            _, high = wm.get_watermark_offsets(TopicPartition(topic, p), timeout=5, cached=False)
        except Exception:                # a brand-new partition may not be known yet
            continue
        total += max(high - pos.get(p, 0), 0)
    return total


# ---------------------------------------------------------------- phase 1-2: raw speeds
def write_test(topic):
    p = Producer(kafka_conf(**PCONF))
    payload = b"x" * MSG_BYTES
    t0 = time.time()
    for i in range(N_WRITE):
        while True:
            try:
                p.produce(topic, payload, key=f"d-{i % DRIVERS:04d}")
                break
            except BufferError:          # local queue full: let it drain
                p.poll(0.05)
        if i % 1000 == 0:
            p.poll(0)
    p.flush(60)
    return N_WRITE / (time.time() - t0)


def read_test(topic):
    c = Consumer(kafka_conf(**{"group.id": f"rd-{uuid.uuid4().hex[:6]}", "auto.offset.reset": "earliest",
                               "enable.auto.commit": False}))
    c.assign([TopicPartition(topic, 0, 0)])
    n, t0, first = 0, time.time(), None
    while n < N_WRITE and time.time() - t0 < 60:
        msgs = c.consume(1000, 1.0)
        if msgs and first is None:
            first = time.time()
        n += sum(1 for m in msgs if not m.error())
    took = time.time() - (first or t0)
    c.close()
    return n / max(took, 0.001)


# ---------------------------------------------------------------- shared consumer
def consumer_loop(topic, group, cap, st, on_msg=None):
    c = Consumer(kafka_conf(**{"group.id": group, "auto.offset.reset": "earliest",
                               "enable.auto.commit": True, "auto.commit.interval.ms": 1000,
                               "topic.metadata.refresh.interval.ms": 2000,
                               "max.poll.interval.ms": 3600000}))
    c.subscribe([topic])
    next_free = time.time()
    while not st["stop"]:
        if S["frozen"]:
            time.sleep(0.1)
            next_free = time.time()
            continue
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        with lock:
            st["pos"][m.partition()] = m.offset() + 1
            st["done"] += 1
            if on_msg:
                on_msg(m)
        next_free = max(next_free, time.time()) + 1.0 / cap
        if next_free > time.time():
            time.sleep(next_free - time.time())
    c.close()


def start_consumers(n, topic, group, cap, st, on_msg=None):
    for _ in range(n):
        threading.Thread(target=consumer_loop, args=(topic, group, cap, st, on_msg), daemon=True).start()


def paced_producer(targets, rate, stop, make_msg, on_delivery=None):
    """Send `rate` msg/s to every topic in `targets`; pauses (and does not catch up) while frozen."""
    p = Producer(kafka_conf(**PCONF))
    sent, t0 = 0, time.time()
    while not stop["stop"]:
        if S["frozen"]:
            f0 = time.time()
            while S["frozen"] and not stop["stop"]:
                p.poll(0.1)
            t0 += time.time() - f0
            continue
        while sent < (time.time() - t0) * rate:
            key, val = make_msg()
            for t in targets:
                p.produce(t, val, key=key, on_delivery=on_delivery)
            sent += 1
        p.poll(0)
        time.sleep(0.01)
    p.flush(10)


# ---------------------------------------------------------------- main
def main():
    global admin, wm
    admin = AdminClient(kafka_conf())
    wm = Consumer(kafka_conf(**{"group.id": f"wm-{uuid.uuid4().hex[:6]}"}))
    t1, t6 = f"lab_part1_{RUN}", f"lab_part6_{RUN}"
    make_topic(t1, 1)
    make_topic(t6, 6)
    line(f"== Lab 17: partition sizing. Topics {t1} (1 partition) and {t6} (6 partitions), RF 3, acks=all")

    # ---- Phase 1: write speed
    i1 = ask(f"One producer sends {N_WRITE:,} messages ({MSG_BYTES} bytes) to the 1-partition topic, then the "
             "same to the 6-partition topic. How much faster is the 6-partition write on this laptop?",
             {"a": "about 6x (six partitions = six times the speed)",
              "b": "somewhat faster (1.2x to 3x)",
              "c": "about the same (less than 1.2x)"})
    line("== Phase 1: writing ...")
    w1 = write_test(t1)
    w6 = write_test(t6)
    ratio = w6 / w1
    line(f"   1 partition : {w1:9,.0f} msg/s  ({w1 * MSG_BYTES / 1e6:5.1f} MB/s)")
    line(f"   6 partitions: {w6:9,.0f} msg/s  ({w6 * MSG_BYTES / 1e6:5.1f} MB/s)   ratio {ratio:.1f}x")
    settle(i1, "a" if ratio >= 4.5 else "b" if ratio >= 1.2 else "c")
    line("   WHY: one partition is already fast: Kafka just appends to a file. Here the 3 brokers share ONE")
    line("   laptop and ONE Python producer, so the client and the disk are the limit, not the partition count.")
    line("   More partitions help writes only when the brokers/disks are the bottleneck. These laptop numbers")
    line("   are not MSK numbers: you always measure on the real hardware (an MSK-only lab later).")
    line(f"   Optional, in Kafka UI: Topics -> {t6} -> Partitions: messages spread over 6")
    wait_enter()

    # ---- Phase 2: read speed
    i2 = ask(f"One consumer reads the {N_WRITE:,} messages back from the 1-partition topic with NO work. "
             f"In Lab 16 a consumer WITH work handled {CAP} msg/s. Raw read speed is roughly:",
             {"a": f"about the same as {CAP} msg/s",
              "b": "10x to 100x faster",
              "c": "more than 100x faster"})
    line("== Phase 2: reading ...")
    r1 = read_test(t1)
    rx = r1 / CAP
    line(f"   raw read: {r1:,.0f} msg/s = {rx:,.0f}x the {CAP} msg/s of a consumer that does real work")
    settle(i2, "c" if rx > 100 else "b" if rx >= 10 else "a")
    line("   WHY: Kafka hands out bytes very fast. What is slow is YOUR code per message (enrich, look up,")
    line("   write to a table). So the partition count is usually set by the slowest CONSUMER, not by Kafka.")
    line("   Formula: partitions = max(target / write per partition, target / read per partition) x headroom")
    wait_enter()

    # ---- Phase 3: sizing
    need = TARGET / CAP
    i3 = ask(f"Target {TARGET:,} msg/s. Writes per partition are {w1:,.0f}/s, one consumer handles {CAP}/s. "
             "With 2x headroom (peaks, catching up after an outage), how many partitions?",
             {"a": "1: the write speed is far above the target",
              "b": f"about 13: {TARGET:,} / {CAP} = {need:.2f}, x2 headroom = {2 * need:.1f}",
              "c": f"{TARGET:,}: one per message per second"}, "b")
    line("   WHY: the consumer side decides. 1 partition = 1 consumer = 200 msg/s, far below the target.")
    line("   Let's test the edge: 6 partitions (6 x 200 = 1,200/s on paper) vs 12 (2,400/s).")
    wait_enter()

    s6t, s12t = f"lab_size6_{RUN}", f"lab_size12_{RUN}"
    make_topic(s6t, 6)
    make_topic(s12t, 12)
    i4 = ask(f"{TARGET:,} msg/s goes into BOTH topics for 30 s, one consumer per partition, {CAP} msg/s each. "
             "What happens?",
             {"a": "both keep up (1,200/s is close enough to 1,250/s)",
              "b": "the 6-partition topic falls further behind every second; 12 keeps up",
              "c": "both fall behind"})
    st6 = {"stop": False, "pos": {}, "done": 0}
    st12 = {"stop": False, "pos": {}, "done": 0}
    pstop = {"stop": False}

    def size_msg():
        d = f"d-{random.randint(0, 4999):04d}"
        return d, json.dumps({"d": d, "ts": time.time()}).encode()

    threading.Thread(target=paced_producer, args=([s6t, s12t], TARGET, pstop, size_msg), daemon=True).start()
    start_consumers(6, s6t, f"size6-{RUN}", CAP, st6)
    start_consumers(12, s12t, f"size12-{RUN}", CAP, st12)
    line("== Phase 3: 30 s (the first seconds include the group joining)")
    t0, last = time.time(), (0, 0, time.time())
    l6 = l12 = 0
    while time.time() - t0 < 30:
        time.sleep(3)
        l6, l12 = lag(s6t, 6, st6["pos"]), lag(s12t, 12, st12["pos"])
        now = time.time()
        r6 = (st6["done"] - last[0]) / (now - last[2])
        r12 = (st12["done"] - last[1]) / (now - last[2])
        last = (st6["done"], st12["done"], now)
        line(f"   [t+{now - t0:3.0f}s]  6 partitions: lag {l6:6,} msgs, read {r6:6,.0f}/s  |  "
             f"12 partitions: lag {l12:6,} msgs, read {r12:6,.0f}/s")
    pstop["stop"] = st6["stop"] = st12["stop"] = True
    behind6, behind12 = l6 > 3 * TARGET, l12 > 3 * TARGET     # more than 3 s of traffic waiting
    settle(i4, "c" if behind6 and behind12 else "b" if behind6 else "a")
    line("   WHY: capacity = partitions x real read speed per consumer. 200/s is the paper number; Lab 16")
    line("   measured about 175/s in Python, so 6 partitions give about 1,050/s < 1,250/s: lag grows FOREVER,")
    line("   it never levels off. 12 partitions have room to spare. That is why we size with headroom, and")
    line("   size on MEASURED read speed, not the nominal one.")
    wait_enter()

    # ---- Phase 4: add partitions to a live keyed topic
    lt = f"lab_grow_{RUN}"
    make_topic(lt, 6)
    st = {"stop": False, "pos": {}, "done": 0}
    pstop = {"stop": False}
    seqs = {}
    part_now = {}
    track = {"grown": False, "seen": {}, "max": {}, "ooo": 0, "dup": 0, "ooo_drivers": set(),
             "ooo_before": 0}

    def live_msg():
        d = f"d-{random.randint(0, DRIVERS - 1):04d}"
        seqs[d] = seqs.get(d, -1) + 1
        return d, json.dumps({"d": d, "seq": seqs[d]}).encode()

    def on_delivery(err, msg):
        if err is None:
            part_now[msg.key().decode()] = msg.partition()

    def on_msg(m):                       # runs under lock
        v = json.loads(m.value())
        d, q = v["d"], v["seq"]
        seen = track["seen"].setdefault(d, set())
        if q in seen:
            track["dup"] += 1
        elif q < track["max"].get(d, -1):
            if track["grown"]:
                track["ooo"] += 1
                track["ooo_drivers"].add(d)
            else:
                track["ooo_before"] += 1
        seen.add(q)
        track["max"][d] = max(track["max"].get(d, -1), q)

    line(f"\n== Phase 4: topic {lt}, 6 partitions, key = driver_id, {DRIVERS:,} drivers, {LIVE_RATE} msg/s")
    line(f"   6 consumers at {LIVE_CAP} msg/s each (= {6 * LIVE_CAP}/s), so a backlog builds up, like a")
    line("   busy evening. Every message carries the driver's own sequence number, so we can check order.")
    threading.Thread(target=paced_producer, args=([lt], LIVE_RATE, pstop, live_msg, on_delivery),
                     daemon=True).start()
    start_consumers(6, lt, f"grow-{RUN}", LIVE_CAP, st, on_msg)
    t0 = time.time()
    while time.time() - t0 < 20:
        time.sleep(4)
        line(f"   [before t+{time.time() - t0:3.0f}s] lag {lag(lt, 6, st['pos']):6,} msgs | "
             f"out-of-order so far: {track['ooo_before']}")

    i5 = ask("We grow the topic from 6 to 12 partitions while it is live. Roughly what share of the "
             f"{DRIVERS:,} drivers now land on a DIFFERENT partition?",
             {"a": "none: Kafka keeps existing keys where they were",
              "b": "about half",
              "c": "almost all of them"}, "b")
    i6 = ask("The old partitions still have a backlog. The new ones start empty. What happens to the order of "
             "pings for drivers who moved?",
             {"a": "nothing: Kafka keeps per-key order across the change",
              "b": "some newer pings are processed BEFORE older ones still waiting in the old partition",
              "c": "the old pings are deleted, so order is fine"}, "b")
    freeze(True)
    before = dict(part_now)
    fs = admin.create_partitions([NewPartitions(lt, 12)])
    for f in fs.values():
        f.result()
    with lock:
        track["grown"] = True
    freeze(False)
    start_consumers(6, lt, f"grow-{RUN}", LIVE_CAP, st, on_msg)
    line("== Phase 4b: now 12 partitions and 12 consumers, waiting for the producer to see them ...")
    t0 = time.time()
    while time.time() - t0 < 20 and not any(p >= 6 for p in list(part_now.values())):
        time.sleep(0.5)
    line(f"   producer started using the new partitions after {time.time() - t0:.1f} s (metadata refresh)")
    part_now.clear()
    t0 = time.time()
    while time.time() - t0 < 60:
        time.sleep(4)
        lg = lag(lt, 12, st["pos"])
        line(f"   [after  t+{time.time() - t0:3.0f}s] lag {lg:6,} msgs | out-of-order: {track['ooo']:,} pings, "
             f"{len(track['ooo_drivers'])} drivers | duplicates: {track['dup']:,}")
        if time.time() - t0 > 20 and lg < 100:
            break
    pstop["stop"] = st["stop"] = True
    time.sleep(1)

    common = [d for d in list(part_now) if d in before]
    moved = sum(1 for d in common if part_now[d] != before[d])
    line(f"\n   drivers that moved partition: {moved} of {len(common)} ({100 * moved / max(len(common), 1):.0f}%)")
    settle(i5, "b")
    line("   WHY: partition = hash(key) mod partition count. Change the count and the answer changes.")
    line("   6 -> 12: a key stays only if hash mod 12 is 0-5, which is about half of them.")
    line(f"   pings processed out of order: {track['ooo']:,} for {len(track['ooo_drivers'])} drivers "
         f"(before the change: {track['ooo_before']})")
    settle(i6, "b")
    line("   WHY: Kafka orders only WITHIN a partition. A moved driver's old pings wait in the old partition's")
    line("   backlog while his new pings sit in a fresh, empty partition and are read at once. For surge that")
    line("   means a driver 'jumps back'; for a compacted topic the old value stays behind in the old partition.")
    line("   Also: new partitions have no committed offset, so auto.offset.reset decides where reading starts.")
    line("   We used 'earliest'. The Java default is 'latest', which skips what was written before the")
    line("   consumer got the partition.")
    if track["dup"]:
        line(f"   The {track['dup']:,} duplicates come from the rebalance (re-reading since the last commit): Lab 18.")
    line(f"   Optional, in Kafka UI: Topics -> {lt} -> Partitions: 6 old ones full, 6 new ones filling")

    line("\n== Your predictions vs results")
    score = 0
    for e in log:
        ok = e["mine"] == e["correct"]
        score += ok
        line(f"   {'RIGHT' if ok else 'wrong'}  you: {e['mine'] or '-'}  answer: {e['correct']}) "
             f"{e['options'][e['correct']][:48]:<48}  {e['q'][:38]}")
    line(f"   score: {score}/{len(log)}")
    line("\n== Answer these 4 in chat")
    line("   SDE2:      how do you choose the number of partitions for a new topic?")
    line("   Principal: the pings topic must grow from 6 to 48 partitions while live. What breaks, and how")
    line("              do you do it safely?")
    line("   Chief Architect: set a partition-sizing standard for every team across 3 countries")
    line("              (prevent / detect / contain / trade-offs).")
    line("   Senior Director: after a partition increase, German surge prices were wrong for 20 minutes.")
    line("              Walk leadership through it (5 parts).")


if __name__ == "__main__":
    main()
