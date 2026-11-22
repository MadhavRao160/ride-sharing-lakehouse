#!/usr/bin/env python3
"""Lab 12: exactly-once (interactive, multiple-choice predictions).
Part 1  a transaction that is aborted, then one that is committed: what two kinds of readers see
        (read_uncommitted vs read_committed), and the hidden marker offsets.
Part 2  read -> process -> write with a crash in the middle:
        A) plain consumer + producer (commit every 20)    -> duplicates after restart
        B) Kafka transactions (offsets committed inside)  -> no duplicates for read_committed readers
Part 3  how Spark + Delta get exactly-once without Kafka transactions (explained, not run).
All topics are new and have 1 partition so offsets are easy to read.
Usage: txn_demo.py"""
import os
import sys
import time
import uuid
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import kafka_conf  # noqa: E402

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

RUN = int(time.time()) % 100000
T_TX1, T_IN, T_OUT_A, T_OUT_B = (f"lab_tx_{n}_{RUN}" for n in ("abort", "in", "out_plain", "out_txn"))
N_IN, BATCH, CRASH_AFTER = 100, 20, 50
log = []


def line(t=""):
    print(t, flush=True)


def pause(ui_hint=None):
    if ui_hint:
        line(f"   Optional, in Kafka UI (http://localhost:8080): {ui_hint}")
    input("   Press Enter to continue... ")
    line()


def ask(q, options, correct):
    line(f"\n?? PREDICT: {q}")
    for k, v in options.items():
        line(f"     {k}) {v}")
    a = input("   Your choice (letter): ").strip().lower()
    line()
    log.append((q, a, correct, options[correct]))


def make_topics():
    admin = AdminClient(kafka_conf())
    fs = admin.create_topics([NewTopic(t, 1, 3) for t in (T_TX1, T_IN, T_OUT_A, T_OUT_B)])
    for f in fs.values():
        f.result()
    time.sleep(2)


def read_all(topic, isolation):
    c = Consumer(kafka_conf(**{"group.id": f"peek-{uuid.uuid4()}", "enable.auto.commit": False,
                               "isolation.level": isolation, "enable.partition.eof": True}))
    c.assign([TopicPartition(topic, 0, 0)])
    vals, deadline = [], time.time() + 20
    while time.time() < deadline:
        m = c.poll(1.0)
        if m is None:
            continue
        if m.error():
            if m.error().code() == KafkaError._PARTITION_EOF:
                break
            continue
        vals.append(m.value().decode())
    c.close()
    return vals


def end_offset(topic):
    c = Consumer(kafka_conf(**{"group.id": f"peek-{uuid.uuid4()}"}))
    hi = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=5, cached=False)[1]
    c.close()
    return hi


def subscribed_consumer(group):
    c = Consumer(kafka_conf(**{"group.id": group, "enable.auto.commit": False,
                               "auto.offset.reset": "earliest", "isolation.level": "read_committed"}))
    c.subscribe([T_IN])          # partitions are assigned during the first polls
    return c


def run_plain(group, last_id):
    """Process input -> T_OUT_A; commit offsets every BATCH messages; stop after input id last_id."""
    c, p = subscribed_consumer(group), Producer(kafka_conf(**{"acks": "all"}))
    while True:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        i = int(m.value().decode())
        p.produce(T_OUT_A, f"processed-{i}".encode())
        p.flush(10)
        if i % BATCH == BATCH - 1:
            c.commit(asynchronous=False)
        if i >= last_id:
            return c


def run_txn(group, txn_id, last_id, crash):
    """Process input -> T_OUT_B in transactions of BATCH; offsets committed inside each transaction."""
    c = subscribed_consumer(group)
    p = Producer(kafka_conf(**{"transactional.id": txn_id, "transaction.timeout.ms": 10000}))
    p.init_transactions()           # also fences and aborts any open transaction of an older producer
    in_txn = False
    while True:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        i = int(m.value().decode())
        if not in_txn:
            p.begin_transaction()
            in_txn = True
        p.produce(T_OUT_B, f"processed-{i}".encode())
        if i % BATCH == BATCH - 1:
            p.send_offsets_to_transaction(c.position(c.assignment()), c.consumer_group_metadata())
            p.commit_transaction()
            in_txn = False
        if i >= last_id:
            break
    if crash:
        p.flush(10)                 # outputs reach Kafka inside an OPEN transaction, then we "crash"
    c.close()
    return p


def dupes(vals):
    return sum(n - 1 for n in Counter(vals).values() if n > 1)


def main():
    make_topics()
    line(f"== Lab 12: exactly-once. Run id {RUN}, all topics have 1 partition")
    line("   at-most-once : may lose, never repeats   | at-least-once: never loses, may repeat")
    line("   exactly-once : each message affects the result once, even through a crash")
    pause()

    # Part 1
    line("== Part 1: a transactional producer writes 10 messages and ABORTS, then 10 more and COMMITS")
    p = Producer(kafka_conf(**{"transactional.id": f"lab-tx-{RUN}-p1"}))
    p.init_transactions()
    p.begin_transaction()
    for i in range(10):
        p.produce(T_TX1, f"aborted-{i}".encode())
    p.flush(10)
    p.abort_transaction()
    p.begin_transaction()
    for i in range(10):
        p.produce(T_TX1, f"committed-{i}".encode())
    p.commit_transaction()

    ask("A reader with isolation.level=read_uncommitted reads from the start. How many messages?",
        {"a": "0", "b": "10", "c": "20"}, "c")
    unc = read_all(T_TX1, "read_uncommitted")
    line(f"   RESULT: {len(unc)} messages ({sum(v.startswith('aborted') for v in unc)} of them from the aborted transaction)")
    line("   WHY: aborting does NOT delete anything. The 10 messages are stored; only a marker says 'aborted'.")
    ask("A reader with isolation.level=read_committed reads from the start. How many messages?",
        {"a": "0", "b": "10", "c": "20"}, "b")
    com = read_all(T_TX1, "read_committed")
    line(f"   RESULT: {len(com)} messages, all from the committed transaction")
    line("   WHY: read_committed skips messages of aborted transactions and waits for open ones.")
    line("   Java's default is read_uncommitted; librdkafka (Python, Go) defaults to read_committed.")
    ask("20 messages were written. What is the topic's end offset (next offset to be written)?",
        {"a": "20", "b": "22", "c": "30"}, "b")
    line(f"   RESULT: end offset = {end_offset(T_TX1)}")
    line("   WHY: each abort and each commit writes one hidden CONTROL MARKER that takes an offset.")
    line("   So on a transactional topic, end offset minus start offset is NOT the number of messages.")
    pause(f"Topics -> {T_TX1} -> Messages: see which messages the UI shows")

    # Part 2A
    line(f"== Part 2A: no transactions. {N_IN} input messages; the app writes each to an output topic")
    line(f"   and commits its input offset every {BATCH}. It CRASHES after {CRASH_AFTER} messages "
         f"(last commit was after message {CRASH_AFTER // BATCH * BATCH - 1}), then restarts.")
    p = Producer(kafka_conf(**{"acks": "all"}))
    for i in range(N_IN):
        p.produce(T_IN, str(i).encode())
    p.flush(10)
    grp_a = f"lab-tx-plain-{RUN}"
    c = run_plain(grp_a, CRASH_AFTER - 1)
    c.close()                                   # crash: offsets after the last commit are not saved
    lost_work = CRASH_AFTER - CRASH_AFTER // BATCH * BATCH
    ask("After the restart finishes all 100, how many DUPLICATES are in the output?",
        {"a": "0", "b": str(lost_work), "c": str(CRASH_AFTER)}, "b")
    c = run_plain(grp_a, N_IN - 1)
    c.close()
    out_a = read_all(T_OUT_A, "read_committed")
    line(f"   RESULT: {len(out_a)} output messages, {len(set(out_a))} unique, {dupes(out_a)} duplicates")
    line(f"   WHY: the restart resumed from the last committed offset, so it redid {lost_work} messages")
    line("   whose output was already written. This is at-least-once: nothing lost, some repeated.")
    pause()

    # Part 2B
    line("== Part 2B: same app with Kafka TRANSACTIONS. Outputs AND the input offset are saved in one")
    line(f"   transaction per {BATCH} messages. Same crash after {CRASH_AFTER}: the last batch's outputs are")
    line("   already in Kafka but its transaction was never committed. On restart, a new producer with the")
    line("   SAME transactional.id fences the old one and aborts its open transaction.")
    grp_b, txn_id = f"lab-tx-txn-{RUN}", f"lab-tx-{RUN}-app"
    _old = run_txn(grp_b, txn_id, CRASH_AFTER - 1, crash=True)
    _new = run_txn(grp_b, txn_id, N_IN - 1, crash=False)
    ask("A read_committed reader reads the output. How many DUPLICATES?",
        {"a": "0", "b": str(lost_work), "c": str(CRASH_AFTER)}, "a")
    out_b = read_all(T_OUT_B, "read_committed")
    line(f"   RESULT: {len(out_b)} output messages, {len(set(out_b))} unique, {dupes(out_b)} duplicates")
    ask("A read_uncommitted reader reads the SAME output. How many messages does it see?",
        {"a": "100", "b": str(N_IN + lost_work), "c": "150"}, "b")
    out_b_u = read_all(T_OUT_B, "read_uncommitted")
    line(f"   RESULT: {len(out_b_u)} messages, {dupes(out_b_u)} duplicates")
    line("   WHY: exactly-once holds only for readers using read_committed. The aborted batch is still")
    line("   stored; a read_uncommitted reader (Java default!) sees it as duplicates.")
    pause()

    # Part 3
    line("== Part 3 (discuss): Spark + Delta, your bronze path, uses NO Kafka transactions")
    line("   1. Before each micro-batch, Spark writes the batch's Kafka offset range to the checkpoint.")
    line("   2. It writes the batch to Delta as one commit, tagged with the batch id.")
    line("   3. After a crash, Spark re-runs the same batch with the same offsets; Delta sees that batch")
    line("      id already committed and skips it. Result: exactly-once into the table.")
    line("   It covers ONE hop (Kafka -> Delta). If the app sent an event twice (Lab 2: 0.99%), both copies")
    line("   are real Kafka messages, so you still need dedup on event_id in silver.")

    line("\n== Your predictions vs results")
    score = 0
    for q, mine, correct, ans in log:
        ok = mine == correct
        score += ok
        line(f"   {'RIGHT' if ok else 'wrong'}  you: {mine or '-'}  answer: {correct}) {ans:<4}  {q[:70]}")
    line(f"   score: {score}/{len(log)}")
    line("\n== Answer these 4 in chat")
    line("   SDE2:      at-most-once vs at-least-once vs exactly-once?")
    line("   Principal: transactions are on; why might a consumer still see aborted messages? What does")
    line("              Spark use instead of Kafka transactions to write exactly once into Delta?")
    line("   Chief Architect: the business says 'zero duplicates end to end'. Where can duplicates still")
    line("              appear in your pipeline, and which control covers each place?")
    line("   Senior Director: finance found 0.1% duplicate payments in a report. How do you respond,")
    line("              and how do you prove it is fixed?")


if __name__ == "__main__":
    main()
