#!/usr/bin/env python3
"""Lab 6: offset commits and crashes.
One consumer on partition 0 of gps_pings, two "lives":
  life 1: read 100 messages, then CRASH (process killed, no clean shutdown)
          mode commit-first : commit, then crash before processing  -> at-most-once
          mode process-first: process, then crash before committing -> at-least-once
  life 2: restart in the same group, continue from Kafka's bookmark, process 100 normally.
Offsets are shown relative to the start, so the first batch is "100-199" as in the question.
Usage: commit_demo.py <commit-first|process-first>"""
import multiprocessing as mp
import os
import sys
import time
import uuid
from collections import Counter

sys.path.insert(0, "/opt/rs/sim_host/simulator")
from kafka_common import kafka_conf  # noqa: E402

TOPIC, PART, BATCH = "gps_pings", 0, 100
mode = sys.argv[1] if len(sys.argv) > 1 else "commit-first"
assert mode in ("commit-first", "process-first"), "mode: commit-first | process-first"
group = f"lab6-{mode}-{uuid.uuid4().hex[:6]}"
log_path = f"/tmp/lab6-{group}.log"


def consumer():
    from confluent_kafka import Consumer
    return Consumer(kafka_conf(**{"group.id": group, "enable.auto.commit": False,
                                  "auto.offset.reset": "earliest"}))


def read_batch(c, n):
    from confluent_kafka import TopicPartition
    c.assign([TopicPartition(TOPIC, PART)])          # resumes at the group's committed offset
    msgs = []
    while len(msgs) < n:
        m = c.poll(5.0)
        if m is not None and not m.error():
            msgs.append(m)
    return msgs


def process(msgs, life):
    with open(log_path, "a") as f:
        for m in msgs:
            f.write(f"{m.offset()} {life}\n")


def commit(c, next_offset):
    from confluent_kafka import TopicPartition
    c.commit(offsets=[TopicPartition(TOPIC, PART, next_offset)], asynchronous=False)


def life1(start):
    c = consumer()
    commit(c, start)                                 # set the bookmark at "100"
    msgs = read_batch(c, BATCH)
    if mode == "commit-first":
        commit(c, msgs[-1].offset() + 1)
        print(f"  life 1: read {rel(msgs[0].offset())}-{rel(msgs[-1].offset())}, "
              f"COMMITTED {rel(msgs[-1].offset() + 1)}, then crashed before processing", flush=True)
    else:
        process(msgs, 1)
        print(f"  life 1: read {rel(msgs[0].offset())}-{rel(msgs[-1].offset())}, "
              f"PROCESSED them, then crashed before committing", flush=True)
    os._exit(1)                                      # the crash: no close, no final commit


def life2():
    c = consumer()
    msgs = read_batch(c, BATCH)
    process(msgs, 2)
    commit(c, msgs[-1].offset() + 1)
    print(f"  life 2: restarted at {rel(msgs[0].offset())}, processed "
          f"{rel(msgs[0].offset())}-{rel(msgs[-1].offset())}, committed {rel(msgs[-1].offset() + 1)}", flush=True)
    c.close()


START = None


def rel(off):
    return off - START + 100


if __name__ == "__main__":
    from confluent_kafka import Consumer, TopicPartition
    probe = Consumer(kafka_conf(**{"group.id": "lab6-probe"}))
    lo, hi = probe.get_watermark_offsets(TopicPartition(TOPIC, PART), timeout=15)
    probe.close()
    START = hi - 5000                                # recent data that already exists
    print(f"mode={mode} group={group} partition={PART} (real start offset {START} shown as 100)")

    p = mp.Process(target=life1, args=(START,))
    p.start()
    p.join()
    print(f"  life 1 exit code: {p.exitcode} (non-zero = crashed)")
    time.sleep(2)
    life2()

    seen = Counter()
    if os.path.exists(log_path):
        with open(log_path) as f:
            for line in f:
                seen[int(line.split()[0])] += 1
    window = range(START, START + 2 * BATCH)
    never = [o for o in window if seen[o] == 0]
    twice = [o for o in window if seen[o] >= 2]
    once = [o for o in window if seen[o] == 1]

    def span(xs):
        return f"{rel(xs[0])}-{rel(xs[-1])}" if xs else "none"
    print(f"\n== result for offsets 100-299")
    print(f"processed exactly once : {len(once):>3}  ({span(once)})")
    print(f"processed TWICE        : {len(twice):>3}  ({span(twice)})")
    print(f"NEVER processed (lost) : {len(never):>3}  ({span(never)})")
    print(f"Kafka UI: Consumers -> {group} shows its committed offset on partition {PART}")
