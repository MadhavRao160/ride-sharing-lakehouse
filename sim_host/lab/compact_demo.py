#!/usr/bin/env python3
"""Lab 10: delete vs compact (interactive).
Makes a new compacted topic (1 partition, RF 3, small segments so compaction runs within the lab),
writes 100 status updates for each of 5 drivers, then shows, step by step:
  1. what a reader sees right after writing
  2. what is left after compaction (and that offsets keep their gaps)
  3. a tombstone (key with an empty value) for driver d3: before and after compaction
  4. the tombstone itself disappearing after delete.retention.ms (60 s here)
At every step you type a prediction first; the end shows your predictions next to the results.
Usage: compact_demo.py"""
import json
import os
import sys
import time
import uuid
from collections import Counter, OrderedDict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

DRIVERS = [f"d{i}" for i in range(1, 6)]
UPDATES = 100
STATUSES = ["available", "en_route_to_pickup", "on_trip", "offline"]
ROLL_KEY = "_roll"            # helper key: a new message makes Kafka close the old segment
TOPIC = f"driver_status_{int(time.time()) % 100000}"
log = []                      # (question, your prediction, actual)


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


def read_all():
    """Read the whole topic from the first offset. Returns list of (offset, key, value or None)."""
    c = Consumer(kafka_conf(**{"group.id": f"peek-{uuid.uuid4()}", "enable.auto.commit": False}))
    tp = TopicPartition(TOPIC, 0)
    low, high = c.get_watermark_offsets(tp, timeout=5, cached=False)
    out = []
    if high > low:
        c.assign([TopicPartition(TOPIC, 0, low)])
        deadline = time.time() + 20
        while time.time() < deadline:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            out.append((m.offset(), m.key().decode(), m.value()))
            if m.offset() >= high - 1:
                break
    c.close()
    return out


def show(recs, title):
    real = [r for r in recs if r[1] != ROLL_KEY]
    per = Counter(r[1] for r in real)
    latest = OrderedDict()
    for off, k, v in real:
        latest[k] = (off, v)
    line(f"   {title}")
    line(f"   messages in the topic: {len(real)}  (plus {len(recs) - len(real)} helper '{ROLL_KEY}' messages)")
    line("   per driver: " + "  ".join(f"{k}={per.get(k, 0)}" for k in DRIVERS))
    for k in DRIVERS:
        if k in latest:
            off, v = latest[k]
            shown = "TOMBSTONE (empty value)" if v is None else json.loads(v)["status"] + \
                f" (update #{json.loads(v)['seq']})"
            line(f"     {k}: latest at offset {off:>3} -> {shown}")
        else:
            line(f"     {k}: GONE (no message left for this key)")
    offs = [r[0] for r in real][:8]
    line(f"   first offsets present: {offs}{' ...' if len(real) > 8 else ''}")
    return real


def produce(p, key, value):
    p.produce(TOPIC, value, key.encode())
    p.flush(10)


def wait_until(p, done, what, max_s):
    """Every 12 s write a helper message (closes the old segment); check every 3 s."""
    line(f"   waiting for compaction ({what}), up to {max_s} s. Compaction runs in the background")
    line("   and only on CLOSED segments, so the script writes a helper message every 12 s to close one.")
    t0, last_roll = time.time(), 0.0
    while time.time() - t0 < max_s:
        if time.time() - last_roll >= 12:
            produce(p, ROLL_KEY, b"roll")
            last_roll = time.time()
        time.sleep(3)
        recs = read_all()
        if done([r for r in recs if r[1] != ROLL_KEY]):
            line(f"   ...done after {time.time() - t0:.0f} s")
            return recs
        line(f"   ...{time.time() - t0:3.0f} s: {len([r for r in recs if r[1] != ROLL_KEY])} messages still there")
    line(f"   ...not finished in {max_s} s (compaction timing is not guaranteed)")
    return read_all()


def main():
    admin = AdminClient(kafka_conf())
    admin.create_topics([NewTopic(TOPIC, 1, 3, config={
        "cleanup.policy": "compact", "segment.ms": "10000",
        "min.cleanable.dirty.ratio": "0.01", "delete.retention.ms": "60000"})])[TOPIC].result()
    time.sleep(2)
    line(f"== Lab 10: delete vs compact. Topic {TOPIC}: 1 partition, RF 3, cleanup.policy=compact")
    line("   delete  = keep everything until it is older than retention (your gps_pings, ride_events)")
    line("   compact = keep only the LATEST value per key, with no time limit (like a table)")
    pause(f"Topics -> {TOPIC} -> Settings: find cleanup.policy=compact")

    # Step 1
    line(f"== Step 1: write {UPDATES} status updates for each of {len(DRIVERS)} drivers "
         f"({UPDATES * len(DRIVERS)} messages), key = driver id")
    p = Producer(kafka_conf(**{"acks": "all"}))
    for seq in range(1, UPDATES + 1):
        for d in DRIVERS:
            v = json.dumps({"driver": d, "status": STATUSES[(seq + int(d[1:])) % 4], "seq": seq})
            p.produce(TOPIC, v.encode(), d.encode())
    p.flush(10)
    a = ask("A brand-new reader reads from the start right now. How many messages does it see?")
    real = show(read_all(), "RESULT right after writing:")
    log.append(("Messages right after writing", a, len(real)))
    line("   WHY: compaction has not run yet. Writing never removes anything; the cleaner works later,")
    line("   in the background. So a compacted topic can hold many old values per key for a while.")
    pause(f"Topics -> {TOPIC} -> Messages: scroll and see d1..d5 repeating")

    # Step 2
    a = ask("After compaction has run, how many driver messages are left, and at which offsets?")
    real = show(wait_until(p, lambda r: len(r) == len(DRIVERS), "1 message per driver", 120),
                "RESULT after compaction:")
    log.append(("Messages after compaction", a, len(real)))
    line("   WHY: the cleaner copied each closed segment, keeping only the newest value per key.")
    line("   Offsets are NOT renumbered: the gaps are where old values were. Consumers just skip them.")
    pause(f"Topics -> {TOPIC} -> Messages: only 5 driver rows left, offsets with gaps")

    # Step 3
    line("== Step 3: driver d3 leaves the platform. Write a TOMBSTONE: key d3, empty value (null)")
    produce(p, "d3", None)
    a = ask("Read again straight away (before compaction). Can a reader still see d3's OLD status? (yes/no)")
    real = show(read_all(), "RESULT right after the tombstone:")
    old_visible = any(k == "d3" and v is not None for _, k, v in real)
    log.append(("d3 old status visible right after tombstone", a, "yes" if old_visible else "no"))
    line("   WHY: the tombstone is just one more message. Until the cleaner runs, d3's old value is")
    line("   still in the log, and a reader from the start sees the old value AND then the tombstone.")
    line("   Readers must treat a null value as 'delete this key'.")
    pause()

    a = ask("After the next compaction, what is left for d3? (old value / tombstone / nothing)")
    real = show(wait_until(p, lambda r: [x for x in r if x[1] == "d3"] and
                           all(v is None for _, k, v in r if k == "d3"),
                           "only the tombstone left for d3", 120), "RESULT after compaction:")
    left = [v for _, k, v in real if k == "d3"]
    log.append(("Left for d3 after compaction", a,
                "nothing" if not left else ("tombstone" if left[-1] is None else "old value")))
    line("   WHY: compaction removed d3's old value but KEEPS the tombstone for delete.retention.ms")
    line("   (60 s in this lab, 24 h by default), so slow readers still learn that d3 was deleted.")
    pause()

    # Step 4
    a = ask("Wait about a minute more. Does the tombstone itself stay forever? (yes/no)")
    real = show(wait_until(p, lambda r: not any(k == "d3" for _, k, _v in r),
                           "tombstone removed after delete.retention.ms", 180), "RESULT at the end:")
    gone = not any(k == "d3" for _, k, _v in real)
    log.append(("Tombstone stays forever", a, "no" if gone else "yes (not yet removed)"))
    line("   WHY: after delete.retention.ms the next compaction drops the tombstone too. d3 is now")
    line("   completely gone from Kafka. A reader slower than delete.retention.ms would MISS the")
    line("   delete and keep d3 forever in its own copy: that is the GDPR trap.")

    line("\n== Your predictions vs results")
    for q, mine, actual in log:
        line(f"   {q:<46} you: {mine or '-':<12} actual: {actual}")
    line("\n== Answer these 4 in chat")
    line("   SDE2:      delete vs compact? What is a tombstone?")
    line("   Principal: why are Debezium CDC topics and __consumer_offsets compacted? Why can a reader")
    line("              still see an old value after a compaction has run?")
    line("   Chief Architect: a team wants the latest driver status kept in Kafka forever instead of a")
    line("              database. When is that good, when is it bad (size, rebuild time, GDPR)?")
    line("   Senior Director: legal asks you to prove a rider's data is gone from Kafka within 30 days.")
    line("              What is your answer and your control?")


if __name__ == "__main__":
    main()
