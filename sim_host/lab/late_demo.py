#!/usr/bin/env python3
"""Lab 11: late and out-of-order events (interactive).
Writes 20,000 GPS pings (1,000 drivers, 6 partitions) to topic lab_late, whose timestamps are
set by Kafka on arrival (LogAppendTime = broker time). Each ping's value carries its event_time
(when the phone recorded it). Network delay is simulated by setting event_time in the past:
  ~92% of pings: 0.2-2 s late (normal), ~6%: 10-60 s (tunnel), ~2%: 1-4 min (phone offline)
Then it reads the topic back and shows:
  1. the measured gap = broker time - event time (typical, p95, p99, worst)
  2. how many pings arrive out of order
  3. for watermarks of 5 s, 30 s, 2 min, 5 min: share of pings dropped and how late results are
You type a prediction before each result; the end shows your predictions next to the results.
Usage: late_demo.py [pings=20000]"""
import json
import os
import random
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

TOPIC = f"lab_late_{int(time.time()) % 100000}"
PARTS = 6
WATERMARKS = [5, 30, 120, 300]
log = []


def line(t=""):
    print(t, flush=True)


def pause(ui_hint=None):
    if ui_hint:
        line(f"   Optional, in Kafka UI (http://localhost:8080): {ui_hint}")
    input("   Press Enter to continue... ")
    line()


def ask(q):
    a = input(f"\n?? PREDICT: {q}\n   Your prediction: ").strip()
    line()
    return a


def delay_s():
    r = random.random()
    if r < 0.92:
        return random.uniform(0.2, 2.0)
    if r < 0.98:
        return random.uniform(10, 60)
    return random.uniform(60, 240)


def pct(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p))]


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
    admin = AdminClient(kafka_conf())             # keep a reference until the topic exists
    admin.create_topics([NewTopic(TOPIC, PARTS, 3, config={
        "message.timestamp.type": "LogAppendTime", "retention.ms": "3600000"})])[TOPIC].result()
    time.sleep(2)
    line(f"== Lab 11: late and out-of-order events. Topic {TOPIC}: {PARTS} partitions, broker time on arrival")
    line("   event time  = when the phone recorded the ping (inside the message value)")
    line("   broker time = when Kafka received it (the Kafka message timestamp)")
    pause()

    line(f"== Step 1: {n:,} pings from 1,000 drivers over about {n / 1000:.0f} s")
    p = Producer(kafka_conf(**{"linger.ms": 5}))
    t0 = time.time()
    for i in range(n):
        while time.time() - t0 < i / 1000:
            p.poll(0)
            time.sleep(0.001)
        d = random.randrange(1000)
        ev = int((time.time() - delay_s()) * 1000)
        p.produce(TOPIC, json.dumps({"driver_id": d, "event_time": ev, "seq": i}).encode(), str(d).encode())
    p.flush(15)
    line("   written.")
    pause(f"Topics -> {TOPIC} -> Messages: compare the Timestamp column with event_time in the value")

    # read back
    c = Consumer(kafka_conf(**{"group.id": f"late-{uuid.uuid4()}", "enable.auto.commit": False}))
    tps = [TopicPartition(TOPIC, i) for i in range(PARTS)]
    highs = {i: c.get_watermark_offsets(tps[i], timeout=5, cached=False)[1] for i in range(PARTS)}
    c.assign([TopicPartition(TOPIC, i, 0) for i in range(PARTS)])
    recs, done, deadline = [], set(i for i in range(PARTS) if highs[i] == 0), time.time() + 60
    while len(done) < PARTS and time.time() < deadline:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        v = json.loads(m.value())
        recs.append((m.partition(), m.offset(), m.timestamp()[1], v["event_time"]))
        if m.offset() >= highs[m.partition()] - 1:
            done.add(m.partition())
    c.close()
    gaps = sorted((b - e) / 1000 for _, _, b, e in recs)

    # Step 2: gap
    a = ask("What share of pings arrive MORE than 5 s after they happened? (a %)")
    late5 = 100 * sum(g > 5 for g in gaps) / len(gaps)
    line("   RESULT: gap = broker time - event time")
    line(f"   typical (p50) {pct(gaps, .5):.1f} s | slow 5% (p95) {pct(gaps, .95):.1f} s | "
         f"slow 1% (p99) {pct(gaps, .99):.1f} s | worst {gaps[-1]:.1f} s")
    line(f"   more than 5 s late: {late5:.1f}%")
    log.append(("% of pings more than 5 s late", a, f"{late5:.1f}%"))
    line("   WHY: most pings are almost on time, but a long tail (tunnels, phones offline) is very late.")
    line("   The average hides the tail; size anything that waits for data from p99 and the worst case.")
    pause()

    # Step 3: out of order
    a = ask("Within one partition, what share of pings arrive with an OLDER event time than a ping "
            "already read before them? (a %)")
    ooo, maxev = 0, {}
    for part, _off, _b, e in sorted(recs, key=lambda r: (r[0], r[1])):
        if e < maxev.get(part, -1):
            ooo += 1
        maxev[part] = max(maxev.get(part, -1), e)
    ooo_pct = 100 * ooo / len(recs)
    line(f"   RESULT: {ooo:,} of {len(recs):,} pings ({ooo_pct:.1f}%) arrived out of order")
    log.append(("% out of order", a, f"{ooo_pct:.1f}%"))
    line("   WHY: Kafka keeps ARRIVAL order per partition, not event order. Any delay that differs from")
    line("   ping to ping mixes the order. The stream job, not Kafka, must sort it out by event time.")
    pause()

    # Step 4: watermark
    a = ask("A surge job uses a 30 s watermark. What % of pings does it drop as too late? (a %)")
    ordered = sorted(recs, key=lambda r: (r[2], r[0], r[1]))      # arrival order across partitions
    results = []
    for w in WATERMARKS:
        maxe, dropped = -1, 0
        for _p, _o, _b, e in ordered:
            maxe = max(maxe, e)
            if e < maxe - w * 1000:
                dropped += 1
        results.append((w, 100 * dropped / len(ordered)))
    line("   RESULT: watermark = (newest event time seen) - W. A ping older than that is dropped.")
    line(f"   {'watermark W':>12}  {'dropped':>8}  {'each minute result is ready about':>34}")
    for w, d in results:
        line(f"   {w:>10} s  {d:>7.2f}%  {f'{w} s after the minute ends':>34}")
    log.append(("% dropped with 30 s watermark", a, f"{dict(results)[30]:.2f}%"))
    line("   WHY: a bigger W drops fewer pings but makes every result later. You cannot have both.")
    line("   Pick W from the measured gap and the business need, and do something with what is dropped.")
    pause()

    a = ask("Which watermark would you pick for SURGE, and what would you do with the dropped pings?")
    log.append(("Watermark for surge", a, "see the answer from Claude"))

    line("\n== Your predictions vs results")
    for q, mine, actual in log:
        line(f"   {q:<34} you: {mine or '-':<14} actual: {actual}")
    line("\n== Answer these 4 in chat")
    line("   SDE2:      event time vs broker time? Which one should surge use, and why?")
    line("   Principal: how do you choose the watermark size? What happens to pings that arrive after it?")
    line("   Chief Architect: surge needs fresh results, finance needs every trip counted. How do you")
    line("              design for both from the same Kafka data?")
    line("   Senior Director: a city's surge was too low for 10 minutes because pings came in late.")
    line("              How do you explain it to the business, and what do you change?")


if __name__ == "__main__":
    main()
