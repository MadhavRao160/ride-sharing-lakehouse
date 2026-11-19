#!/usr/bin/env python3
"""Lab 7: replay by moving a group's bookmark (committed offsets).
Steps (run one at a time, look at Kafka UI -> Consumers -> lab7 in between):
  fill            set the bookmark to the end of every partition -> lag 0
  reset <minutes> move the bookmark back to "now - minutes" (by broker timestamp)
  replay          read from the bookmark to the end, report what came back and how fast
The group has no live members between steps, which a reset requires.
Usage: replay_demo.py fill | reset <minutes> | replay   [group=lab7]"""
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

TOPIC = "gps_pings"
step = sys.argv[1] if len(sys.argv) > 1 else "fill"
arg = sys.argv[2] if len(sys.argv) > 2 else None
group = sys.argv[3] if len(sys.argv) > 3 else "lab7"


def ts(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


c = Consumer(kafka_conf(**{"group.id": group, "enable.auto.commit": False,
                           "auto.offset.reset": "earliest"}))
parts = sorted(c.list_topics(TOPIC, timeout=15).topics[TOPIC].partitions)
tps = [TopicPartition(TOPIC, p) for p in parts]
marks = {p: c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=15) for p in parts}


def lag_report(title):
    committed = {tp.partition: tp.offset for tp in c.committed(tps, timeout=15)}
    total = 0
    print(title)
    for p in parts:
        lo, hi = marks[p]
        off = committed[p]
        lag = hi - off if off >= 0 else hi - lo
        total += lag
        print(f"  p{p}: bookmark={off:>9}  oldest_kept={lo:>9}  end={hi:>9}  lag={lag:>9,}")
    print(f"  TOTAL lag = {total:,} messages")


def read_to_end():
    """Read every partition from the group's bookmark up to the end offset seen at start."""
    committed = {tp.partition: tp.offset for tp in c.committed(tps, timeout=15)}
    start = {p: (committed[p] if committed[p] >= 0 else marks[p][0]) for p in parts}
    stop = {p: marks[p][1] for p in parts}
    todo = [p for p in parts if start[p] < stop[p]]
    pos = dict(start)
    n, first_ts, t0 = 0, None, time.time()
    if todo:
        c.assign([TopicPartition(TOPIC, p, start[p]) for p in todo])
    while todo and time.time() - t0 < 300:
        for m in c.consume(1000, 2.0):
            p = m.partition()
            if m.error() or p not in todo or m.offset() >= stop[p]:
                continue
            n += 1
            if first_ts is None or m.timestamp()[1] < first_ts:
                first_ts = m.timestamp()[1]
            pos[p] = m.offset() + 1
            if pos[p] >= stop[p]:
                todo.remove(p)
                c.pause([TopicPartition(TOPIC, p)])
    c.commit(offsets=[TopicPartition(TOPIC, p, pos[p]) for p in parts], asynchronous=False)
    return n, first_ts, time.time() - t0


if step == "fill":
    c.commit(offsets=[TopicPartition(TOPIC, p, marks[p][1]) for p in parts], asynchronous=False)
    print(f"fill: group {group} is now caught up (bookmark = end of every partition)")
    lag_report("after fill:")

elif step == "reset":
    minutes = float(arg or 30)
    target = int(time.time() * 1000 - minutes * 60_000)
    found = c.offsets_for_times([TopicPartition(TOPIC, p, target) for p in parts], timeout=15)
    new = []
    print(f"reset: target time {ts(target)} ({minutes:g} min ago)")
    for tp in found:
        lo, hi = marks[tp.partition]
        off = tp.offset if tp.offset >= 0 else hi       # -1 = nothing that new: go to the end
        note = "  <- older than anything kept: clamped to the oldest message" if off == lo else ""
        new.append(TopicPartition(TOPIC, tp.partition, off))
        print(f"  p{tp.partition}: bookmark -> {off}{note}")
    c.commit(offsets=new, asynchronous=False)
    lag_report("after reset:")

elif step == "replay":
    lag_report("before replay:")
    n, first_ts, secs = read_to_end()
    print(f"\nreplay: read {n:,} messages in {secs:.1f}s ({n / max(secs, 0.01):,.0f} msgs/s)")
    if first_ts:
        print(f"  first message replayed was written at {ts(first_ts)}")
    lag_report("after replay:")

c.close()
