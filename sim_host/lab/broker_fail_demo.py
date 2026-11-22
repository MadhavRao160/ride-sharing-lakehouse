#!/usr/bin/env python3
"""Lab 13: one broker fails (interactive, multiple-choice predictions). LOCAL MODE ONLY.
A producer (acks=all, idempotent) writes 200 msg/s with sequence numbers to a new topic
(3 partitions, RF 3, min.insync.replicas 2) and counts every message sent / confirmed / failed.
Phases, with a status line every 2 s (confirmed per second, ISR size per partition):
  1. normal            2. KILL the leader broker of partition 0 (docker kill = sudden crash)
  3. KILL a second broker (only 1 copy left)   4. START both again, wait until ISR = 3
At the end it reads the topic back: are any CONFIRMED messages missing?
Usage: broker_fail_demo.py"""
import os
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import MODE, kafka_conf  # noqa: E402

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

TOPIC = f"lab_bf_{int(time.time()) % 100000}"
PARTS, RATE = 3, 200
PORT_TO_CONTAINER = {9092: "kafka1", 9192: "kafka2", 9292: "kafka3"}
log = []
st = {"sent": 0, "ok": 0, "fail": 0, "ok_ids": set(), "fail_ids": set(), "errors": {}}


def line(t=""):
    print(t, flush=True)


def ask(q, options, correct):
    line(f"\n?? PREDICT: {q}")
    for k, v in options.items():
        line(f"     {k}) {v}")
    a = input("   Your choice (letter): ").strip().lower()
    line()
    log.append((q, a, correct, options[correct]))


def on_delivery(err, msg):
    i = int(msg.value().decode())
    if err is None:
        st["ok"] += 1
        st["ok_ids"].add(i)
    else:
        st["fail"] += 1
        st["fail_ids"].add(i)
        st["errors"][err.name()] = st["errors"].get(err.name(), 0) + 1


def containers(admin):
    md = admin.list_topics(timeout=10)
    return {b.id: PORT_TO_CONTAINER.get(b.port, f"kafka{b.id}") for b in md.brokers.values()}


def isr_view(admin):
    try:
        t = admin.list_topics(TOPIC, timeout=2).topics[TOPIC]
        return " ".join(f"p{p}:L{t.partitions[p].leader}/ISR{len(t.partitions[p].isrs)}"
                        for p in sorted(t.partitions))
    except Exception:
        return "metadata unavailable"


def run_phase(p, admin, seconds, label):
    """Produce at RATE for `seconds`, print a status line every 2 s."""
    t0 = time.time()
    next_print, ok_before = t0 + 2, st["ok"]
    while time.time() - t0 < seconds:
        due = int((time.time() - t0) * RATE)
        while st["phase_sent"] < due:
            try:
                p.produce(TOPIC, str(st["sent"]).encode(), on_delivery=on_delivery)
                st["sent"] += 1
                st["phase_sent"] += 1
            except BufferError:
                break
        p.poll(0.05)
        if time.time() >= next_print:
            rate = (st["ok"] - ok_before) / 2
            ok_before = st["ok"]
            line(f"   [{label:<11} t+{time.time() - t0:4.0f}s] sent {st['sent']:>6,}  confirmed {st['ok']:>6,}  "
                 f"failed {st['fail']:>5,}  confirmed/s {rate:5.0f}  {isr_view(admin)}")
            next_print += 2
    st["phase_sent"] = 0


def docker(action, name):
    line(f"   >>> docker {action} {name}")
    subprocess.run(["docker", action, name], check=True, capture_output=True)


def main():
    if MODE != "local":
        sys.exit("Lab 13 kills Docker brokers: run it only with ./local/lab.sh (KAFKA_MODE=local)")
    admin = AdminClient(kafka_conf())
    admin.create_topics([NewTopic(TOPIC, PARTS, 3, config={"min.insync.replicas": "2"})])[TOPIC].result()
    time.sleep(2)
    names = containers(admin)
    t = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
    victim1 = t.partitions[0].leader
    victim2 = next(b for b in sorted(names) if b != victim1)
    line(f"== Lab 13: one broker fails. Topic {TOPIC}: {PARTS} partitions, RF 3, min.insync.replicas 2")
    line(f"   brokers: {names}. Producer: acks=all, idempotent, gives up on a message after 15 s.")
    line("   Status columns: L = leader broker id, ISR = number of copies that are in sync")
    st["phase_sent"] = 0
    p = Producer(kafka_conf(**{"acks": "all", "enable.idempotence": True, "message.timeout.ms": 15000}))

    line("\n== Phase 1: normal traffic, 10 s")
    run_phase(p, admin, 10, "normal")

    ask(f"Broker {victim1} ({names[victim1]}) leads partition 0 and is about to CRASH. What happens to writes?",
        {"a": "they stop until the broker comes back",
         "b": "they pause a few seconds, then continue",
         "c": "nothing at all, no pause"}, "b")
    docker("kill", names[victim1])
    line("\n== Phase 2: one broker down, 20 s")
    run_phase(p, admin, 20, "1 down")
    line("   WHY: a follower in the ISR became leader (a few seconds). 2 copies are still >= min.insync 2,")
    line("   so acks=all writes keep succeeding. The ISR shows 2: partitions are UNDER-REPLICATED.")
    line(f"   Optional, in Kafka UI (http://localhost:8080): Brokers, and Topics -> {TOPIC} -> partitions")
    input("   Press Enter to continue... ")

    ask(f"Now broker {victim2} ({names[victim2]}) crashes too. Only 1 copy is left. What happens to writes?",
        {"a": "they continue normally",
         "b": "Kafka refuses them: confirmed/s drops to 0 and messages start to fail",
         "c": "they continue, and some are lost later"}, "b")
    docker("kill", names[victim2])
    line("\n== Phase 3: two brokers down, 25 s")
    run_phase(p, admin, 25, "2 down")
    line("   WHY: 1 copy < min.insync.replicas 2, so the broker refuses acks=all writes")
    line("   (NOT_ENOUGH_REPLICAS) rather than accept data it could lose. The producer retries, then gives")
    line("   up after 15 s and reports the message as FAILED: the app knows, nothing is lost silently.")
    line("   (D) In this lab each broker is also a KRaft controller, so with 2 of 3 down the cluster also")
    line("   cannot elect new leaders until one returns.")
    line(f"   errors seen so far: {st['errors'] or 'none'}")
    input("   Press Enter to continue... ")

    docker("start", names[victim2])
    docker("start", names[victim1])
    line("\n== Phase 4: both brokers restarted; producing until every partition has ISR 3 again (max 90 s)")
    t_back = time.time()
    while time.time() - t_back < 90:
        run_phase(p, admin, 4, "recovering")
        v = isr_view(admin)
        if v.count("ISR3") == PARTS:
            line(f"   all partitions back to 3 in-sync copies after {time.time() - t_back:.0f} s")
            break
    run_phase(p, admin, 4, "normal")
    p.flush(30)

    ask("Reading the topic back: how many CONFIRMED messages are missing?",
        {"a": "0", "b": "a few, from the moment of the crash", "c": "all those written while brokers were down"},
        "a")
    c = Consumer(kafka_conf(**{"group.id": f"bf-{uuid.uuid4()}", "enable.partition.eof": True}))
    c.assign([TopicPartition(TOPIC, i, 0) for i in range(PARTS)])
    seen, eof, deadline = set(), set(), time.time() + 60
    while len(eof) < PARTS and time.time() < deadline:
        m = c.poll(1.0)
        if m is None:
            continue
        if m.error():
            if m.error().code() == KafkaError._PARTITION_EOF:
                eof.add(m.partition())
            continue
        seen.add(int(m.value().decode()))
    c.close()
    missing = st["ok_ids"] - seen
    failed_present = st["fail_ids"] & seen
    line(f"   RESULT: sent {st['sent']:,} | confirmed {st['ok']:,} | failed {st['fail']:,} | in the topic {len(seen):,}")
    line(f"   confirmed but MISSING: {len(missing)}")
    line(f"   failed but actually in the topic anyway: {len(failed_present)} of {len(st['fail_ids'])}")
    line("   WHY: with RF 3, min.insync 2, acks=all and unclean leader election off, a confirmed message")
    line("   is on at least 2 brokers, so losing 1 broker loses nothing. 'Failed' means NOT confirmed: the")
    line("   app must retry or store it. Some 'failed' ones may still be in the topic (the outcome was")
    line("   unknown when the producer gave up), which is why consumers still dedup.")
    line(f"   errors: {st['errors'] or 'none'}")

    line("\n== Your predictions vs results")
    score = 0
    for q, mine, correct, ans in log:
        ok = mine == correct
        score += ok
        line(f"   {'RIGHT' if ok else 'wrong'}  you: {mine or '-'}  answer: {correct}) {ans[:45]:<45}  {q[:50]}")
    line(f"   score: {score}/{len(log)}")
    line("\n== Answer these 4 in chat")
    line("   SDE2:      what happens to writes when 1 of 3 brokers dies? What is the ISR?")
    line("   Principal: why does Kafka refuse writes with 2 of 3 brokers down? What would acks=1 do?")
    line("   Chief Architect: on MSK, how do you place brokers so one availability zone can fail without")
    line("              stopping writes? What does RF 3 cost you?")
    line("   Senior Director: AWS patches brokers one at a time every month. How do you make sure that")
    line("              causes no outage and no data loss, and how do you prove it?")


if __name__ == "__main__":
    main()
