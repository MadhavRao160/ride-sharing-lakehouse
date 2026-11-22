#!/usr/bin/env python3
"""Lab 15: unclean leader election (interactive, multiple-choice predictions). LOCAL MODE ONLY.
Topic: 1 partition, 2 copies, on broker 2 (leader) and broker 3 only. min.insync.replicas 1.
  1. write 100 messages (ids 0-99): both copies have them
  2. FREEZE broker 3 (docker pause): it drops out of the ISR. Write 200 more (ids 100-299):
     confirmed, but stored ONLY on broker 2
  3. CRASH broker 2, unfreeze broker 3. Unclean election OFF: the partition goes offline
  4. turn unclean election ON: stale broker 3 becomes leader. Count what is lost.
     Then write 50 new messages (ids 1000-1049): which offsets do they get?
  5. START broker 2 again: does it bring the 200 back?
Usage: ./local/lab.sh unclean_demo.py"""
import os
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import MODE, kafka_conf  # noqa: E402

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

TOPIC = f"lab_ule_{int(time.time()) % 100000}"
LEADER, STALE = 2, 3                       # broker ids = KAFKA_NODE_ID in docker-compose
NAME = {1: "kafka1", 2: "kafka2", 3: "kafka3"}
log = []
st = {"ok": set(), "fail": set(), "offset": {}, "errors": {}}


def line(t=""):
    print(t, flush=True)


def ask(q, options, correct):
    line(f"\n?? PREDICT: {q}")
    for k, v in options.items():
        line(f"     {k}) {v}")
    a = input("   Your choice (letter): ").strip().lower()
    line()
    log.append((q, a, correct, options[correct]))


def pause():
    input("   Press Enter to continue... ")


def docker(*args, check=True):
    line(f"   >>> docker {' '.join(args)}")
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def kafka_tool(*args):
    """Run a Kafka CLI tool inside kafka1 (always up in this lab)."""
    r = docker("exec", "kafka1", *args, "--bootstrap-server", "kafka1:19092", check=False)
    out = (r.stdout + r.stderr).strip()
    if out:
        line("       " + out.replace("\n", "\n       "))


def state(admin):
    try:
        p = admin.list_topics(TOPIC, timeout=3).topics[TOPIC].partitions[0]
        return p.leader, sorted(p.isrs)
    except Exception:
        return None, []


def show(admin, label):
    leader, isr = state(admin)
    who = "NONE (offline)" if leader in (-1, None) else f"broker {leader}"
    line(f"   [{label}] leader: {who}   in-sync copies (ISR): {isr or '-'}")


def wait_for(admin, test, label, max_s):
    t0 = time.time()
    while time.time() - t0 < max_s:
        leader, isr = state(admin)
        if test(leader, isr):
            line(f"   ...{label} after {time.time() - t0:.0f} s")
            return True
        time.sleep(2)
    line(f"   ...{label}: NOT reached in {max_s} s (continuing anyway)")
    return False


def on_delivery(err, msg):
    i = int(msg.value().decode())
    if err is None:
        st["ok"].add(i)
        st["offset"][i] = msg.offset()
    else:
        st["fail"].add(i)
        st["errors"][err.name()] = st["errors"].get(err.name(), 0) + 1


def produce(ids, timeout_ms=15000):
    """New producer each time (idempotent state does not survive an unclean election)."""
    p = Producer(kafka_conf(**{"acks": "all", "enable.idempotence": True,
                               "message.timeout.ms": timeout_ms}))
    for i in ids:
        p.produce(TOPIC, str(i).encode(), on_delivery=on_delivery)
        p.poll(0)
    p.flush(timeout_ms / 1000 + 10)
    ok = sum(1 for i in ids if i in st["ok"])
    line(f"   wrote ids {ids[0]}-{ids[-1]}: confirmed {ok} of {len(ids)}, failed {len(ids) - ok}")
    return ok


def read_all(max_s=20):
    """Read the partition from offset 0 to the end: list of (offset, id)."""
    c = Consumer(kafka_conf(**{"group.id": f"ule-{uuid.uuid4()}", "enable.partition.eof": True,
                               "enable.auto.commit": False}))
    c.assign([TopicPartition(TOPIC, 0, 0)])
    rows, deadline = [], time.time() + max_s
    while time.time() < deadline:
        m = c.poll(1.0)
        if m is None:
            continue
        if m.error():
            if m.error().code() == KafkaError._PARTITION_EOF:
                break
            continue
        rows.append((m.offset(), int(m.value().decode())))
    c.close()
    return rows


def ranges(nums):
    """[100,101,102,150] -> '100-102, 150'"""
    nums, out = sorted(nums), []
    for n in nums:
        if out and n == out[-1][1] + 1:
            out[-1][1] = n
        else:
            out.append([n, n])
    return ", ".join(f"{a}-{b}" if a != b else f"{a}" for a, b in out) or "none"


def main():
    if MODE != "local":
        sys.exit("Lab 15 pauses and kills Docker brokers: run it only with ./local/lab.sh (KAFKA_MODE=local)")
    admin = AdminClient(kafka_conf())
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replica_assignment=[[LEADER, STALE]],
                                  config={"min.insync.replicas": "1",
                                          "unclean.leader.election.enable": "false"})])[TOPIC].result()
    time.sleep(3)
    line(f"== Lab 15: unclean leader election. Topic {TOPIC}: 1 partition, 2 copies (broker {LEADER} and "
         f"broker {STALE}), min.insync.replicas 1, unclean election OFF")
    line("   Broker 1 holds no copy; it only keeps the controller quorum alive.")
    line("   (D) min.insync 1 is on purpose: it lets the leader confirm writes alone, which is the trap.")
    show(admin, "start")

    # ---------------- Phase 1
    line("\n== Phase 1: write 100 messages with both copies in sync")
    produce(list(range(0, 100)))
    show(admin, "phase 1")

    # ---------------- Phase 2
    line(f"\n== Phase 2: FREEZE broker {STALE} (like a long GC pause or a network cut)")
    docker("pause", NAME[STALE])
    wait_for(admin, lambda l, isr: isr == [LEADER], f"broker {STALE} dropped out of the ISR", 60)
    show(admin, "phase 2")
    ask(f"Only broker {LEADER} is in sync now. We write 200 more (acks=all). What happens?",
        {"a": "Kafka refuses them: not enough copies",
         "b": f"they are confirmed, but stored only on broker {LEADER}",
         "c": f"they wait until broker {STALE} is back"}, "b")
    produce(list(range(100, 300)))
    line(f"   WHY: acks=all means 'all copies in the ISR', and the ISR is just broker {LEADER}. With")
    line("   min.insync.replicas 1 that is enough, so 300 messages are confirmed, 200 of them on ONE disk.")
    line("   Our real topics use min.insync 2, so these writes would have been refused instead (Lab 13).")
    pause()

    # ---------------- Phase 3
    ask(f"Broker {LEADER} (the only up-to-date copy) now CRASHES and broker {STALE} wakes up. "
        "Unclean election is OFF. What happens?",
        {"a": f"broker {STALE} becomes leader automatically",
         "b": "the partition goes offline: no reads, no writes, but nothing is lost",
         "c": "broker 1 takes over the partition"}, "b")
    docker("kill", NAME[LEADER])
    time.sleep(3)
    docker("unpause", NAME[STALE])
    line(f"\n== Phase 3: broker {LEADER} down, broker {STALE} back but missing ids 100-299")
    wait_for(admin, lambda l, isr: l == -1, "the partition has no leader", 60)
    show(admin, "phase 3")
    line("   Trying to write 20 messages (ids 500-519, give up after 10 s):")
    produce(list(range(500, 520)), timeout_ms=10000)
    rows = read_all(10)
    line(f"   Trying to read: got {len(rows)} messages")
    line(f"   WHY: only ISR members may become leader. Broker {STALE} is not in the ISR (it is missing data),")
    line("   so Kafka chooses to be UNAVAILABLE rather than lose confirmed messages. This is the default.")
    line(f"   Optional, in Kafka UI (http://localhost:8080): Topics -> {TOPIC} -> partition 0 has no leader")
    pause()

    # ---------------- Phase 4
    ask("We now turn unclean election ON, so the stale broker becomes leader. "
        "Of the 300 CONFIRMED messages, how many can consumers read?",
        {"a": "all 300", "b": "100 (ids 0-99)", "c": "0"}, "b")
    line("\n== Phase 4: unclean election ON")
    kafka_tool("/opt/kafka/bin/kafka-configs.sh", "--alter", "--entity-type", "topics", "--entity-name", TOPIC,
               "--add-config", "unclean.leader.election.enable=true")
    line("   (In KRaft, turning it on does not elect at once; the check runs every 5 min. So we force it:)")
    kafka_tool("/opt/kafka/bin/kafka-leader-election.sh", "--election-type", "unclean",
               "--topic", TOPIC, "--partition", "0")
    wait_for(admin, lambda l, isr: l == STALE, f"broker {STALE} is leader", 60)
    show(admin, "phase 4")
    seen = {i for _, i in read_all()}
    confirmed = {i for i in st["ok"] if i < 300}
    lost = confirmed - seen
    line(f"   confirmed {len(confirmed)} | readable now {len(seen)} | confirmed but GONE {len(lost)} "
         f"(ids {ranges(lost)})")
    line("   Now write 50 NEW messages (ids 1000-1049):")
    produce(list(range(1000, 1050)))
    new_off = sorted(st["offset"][i] for i in range(1000, 1050) if i in st["offset"])
    if new_off:
        line(f"   the new messages got offsets {new_off[0]}-{new_off[-1]}")
    line(f"   WHY: the new leader only has ids 0-99, so its log ends at offset 100. New messages reuse")
    line("   offsets 100+. Offset 120 used to mean id 120; now it means a different message. A consumer")
    line("   that had already read offset 120 sees different data there than a new reader.")
    pause()

    # ---------------- Phase 5
    ask(f"Broker {LEADER} comes back. It still has ids 100-299 on its disk. What happens to them?",
        {"a": f"they are copied back to broker {STALE}: all 300 return",
         "b": f"broker {LEADER} deletes them too: gone for good",
         "c": f"they stay on broker {LEADER} only and can be read on request"}, "b")
    line(f"\n== Phase 5: start broker {LEADER} again")
    docker("start", NAME[LEADER])
    wait_for(admin, lambda l, isr: len(isr) == 2, "both copies in sync again", 120)
    show(admin, "phase 5")
    rows = read_all()
    seen = {i for _, i in rows}
    gone = confirmed - seen
    line(f"   RESULT: in the topic now {len(rows)} messages | confirmed ids missing for good: {len(gone)} "
         f"({ranges(gone)})")
    sample = [(o, i) for o, i in rows if 100 <= o <= 102]
    if sample:
        line(f"   offsets 100-102 now hold ids {[i for _, i in sample]} (they used to hold 100, 101, 102)")
    line(f"   WHY: broker {LEADER} rejoins as a FOLLOWER. A follower must match its leader, so it cuts its log")
    line(f"   back to where it differs from broker {STALE} (offset 100) and copies from there. The 200 messages")
    line("   are deleted from the last disk that had them. Nobody got an error: this is SILENT loss.")
    line("   Safe settings (ours): unclean election OFF, RF 3, min.insync 2, acks=all.")

    line("\n== Your predictions vs results")
    score = 0
    for q, mine, correct, ans in log:
        ok = mine == correct
        score += ok
        line(f"   {'RIGHT' if ok else 'wrong'}  you: {mine or '-'}  answer: {correct}) {ans[:50]:<50}  {q[:45]}")
    line(f"   score: {score}/{len(log)}")
    line("\n== Answer these 4 in chat")
    line("   SDE2:      what is unclean leader election, and what is the default?")
    line("   Principal: how can a message the producer got an 'ok' for still disappear?")
    line("   Chief Architect: which topics in our design would you ever allow it on, and why?")
    line("   Senior Director: a partition is offline for 20 min during surge hours; on-call wants to turn")
    line("              unclean election on. Do you approve?")


if __name__ == "__main__":
    try:
        main()
    finally:   # always leave all three brokers running
        for n in ("kafka2", "kafka3"):
            subprocess.run(["docker", "unpause", n], capture_output=True)
            subprocess.run(["docker", "start", n], capture_output=True)
