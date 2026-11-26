#!/usr/bin/env python3
"""Lab 18: rebalance storms (LOCAL Docker Kafka only).

Run:  ./local/lab.sh rebalance_demo.py

Phases (each makes its own fresh 12-partition topic and consumer group):
  1a  Eager rebalance (range): a 5th consumer joins a group of 4.
  1b  Cooperative rebalance (cooperative-sticky): same thing.
  2   Slow processing: one message takes 10 s, limit (max.poll.interval.ms) is 7 s.
  3a  Rolling restart of 4 consumers, normal (dynamic) membership.
  3b  Same rolling restart with static membership (group.instance.id).
Each phase: you predict (multiple choice), it runs, then you see the result and WHY.
"""
import json
import multiprocessing as mp
import os
import queue as pyqueue
import random
import sys
import threading
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOT = "localhost:9092,localhost:9192,localhost:9292"
PARTS = 12
RATE = 500  # messages per second from the producer


# ---------------------------------------------------------------- processes
def producer_proc(topic, stop, slow_at, t0):
    p = Producer({"bootstrap.servers": BOOT, "linger.ms": 20, "log_level": 2})
    slow = sorted(slow_at)
    start, n = time.time(), 0
    while not stop.is_set():
        now = time.time()
        if slow and now - t0 >= slow[0]:
            slow.pop(0)
            p.produce(topic, key="d-slow", value=json.dumps({"ts": now, "slow": True}))
        target = int((now - start) * RATE)
        while n < target:
            p.produce(topic, key=f"d-{random.randrange(1000)}", value=b"{}")
            n += 1
        p.poll(0)
        time.sleep(0.01)
    p.flush(10)


def consumer_proc(name, topic, group, extra, q, stop, slow_s):
    conf = {
        "bootstrap.servers": BOOT,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 1000,
        "log_level": 2,
    }
    conf.update(extra)
    c = Consumer(conf)

    def on_assign(_c, parts):
        q.put(("assign", name, [x.partition for x in parts], time.time()))

    def on_revoke(_c, parts):
        q.put(("revoke", name, [x.partition for x in parts], time.time()))

    c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    buf, last = [], time.time()

    def flush():
        nonlocal buf, last
        if buf:
            q.put(("recs", name, buf, time.time()))
        buf, last = [], time.time()

    try:
        while not stop.is_set():
            m = c.poll(0.2)
            if m is not None:
                if m.error():
                    if m.error().code() == KafkaError._MAX_POLL_EXCEEDED:
                        q.put(("kicked", name, [], time.time()))
                else:
                    buf.append((m.partition(), m.offset(), time.time()))
                    if slow_s and m.key() == b"d-slow":
                        flush()
                        q.put(("slow", name, [m.partition()], time.time()))
                        time.sleep(slow_s)  # the "slow work" that blocks the poll loop
            if time.time() - last > 0.3:
                flush()
    finally:
        flush()
        c.close()


# ---------------------------------------------------------------- lab helper
class Lab:
    def __init__(self, tag):
        self.topic = f"lab18-{tag}-{uuid.uuid4().hex[:6]}"
        self.group = "g-" + self.topic
        a = AdminClient({"bootstrap.servers": BOOT})
        fut = a.create_topics([NewTopic(self.topic, num_partitions=PARTS, replication_factor=3)])
        fut[self.topic].result(timeout=30)
        time.sleep(2)
        self.q = mp.Queue()
        self.events = []
        self.procs = {}
        self._run = True
        self._t = threading.Thread(target=self._collect, daemon=True)
        self._t.start()
        print(f"  topic {self.topic}  (Kafka UI: localhost:8080 > Consumers > {self.group})")

    def _collect(self):
        while self._run:
            try:
                self.events.append(self.q.get(timeout=0.2))
            except pyqueue.Empty:
                pass

    def producer(self, slow_at=()):
        stop, t0 = mp.Event(), time.time()
        p = mp.Process(target=producer_proc, args=(self.topic, stop, list(slow_at), t0), daemon=True)
        p.start()
        self.procs["producer"] = (p, stop)

    def consumer(self, name, extra, slow_s=0.0, iid=None):
        cfg = dict(extra)
        if iid:
            cfg["group.instance.id"] = iid
        stop = mp.Event()
        p = mp.Process(target=consumer_proc,
                       args=(name, self.topic, self.group, cfg, self.q, stop, slow_s), daemon=True)
        p.start()
        self.procs[name] = (p, stop)

    def stop(self, name):
        p, s = self.procs.pop(name)
        s.set()
        p.join(25)
        if p.is_alive():
            p.terminate()

    def close(self):
        for n in [k for k in self.procs if k != "producer"]:
            self.stop(n)
        if "producer" in self.procs:
            self.stop("producer")
        time.sleep(1)
        self._run = False
        self._t.join(2)

    # ---- views on collected events
    def ev(self, kind, since=0.0, names=None):
        return [e for e in list(self.events)
                if e[0] == kind and e[3] >= since and (names is None or e[1] in names)]

    def assignment(self):
        st = {}
        for kind, name, parts, _t in list(self.events):
            if kind == "assign":
                st.setdefault(name, set()).update(parts)
            elif kind == "revoke":
                st.setdefault(name, set()).difference_update(parts)
        return st

    def wait_stable(self, names, timeout=60, quiet=3):
        end = time.time() + timeout
        while time.time() < end:
            st = self.assignment()
            owned = set().union(*[st.get(n, set()) for n in names])
            last = max([e[3] for e in self.events if e[0] in ("assign", "revoke")], default=0)
            if all(st.get(n) for n in names) and len(owned) == PARTS and time.time() - last > quiet:
                return True
            time.sleep(0.5)
        print("  (warning: group did not settle in time; numbers may be partial)")
        return False

    def times(self, a, b):
        out = []
        for e in self.ev("recs"):
            out.extend(r[2] for r in e[2] if a <= r[2] <= b)
        return sorted(out)

    def longest_gap(self, a, b):
        ts = self.times(a, b)
        return (b - a) if len(ts) < 2 else max(y - x for x, y in zip(ts, ts[1:]))

    def idle_total(self, a, b, thr=0.5):
        ts = self.times(a, b)
        return sum(g for g in (y - x for x, y in zip(ts, ts[1:])) if g > thr)

    def duplicates(self):
        c = Counter()
        for e in self.ev("recs"):
            c.update((r[0], r[1]) for r in e[2])
        return sum(v - 1 for v in c.values() if v > 1)

    def clusters(self, kind, since, gap=1.5):
        ts = sorted(e[3] for e in self.ev(kind, since))
        if not ts:
            return 0
        return 1 + sum(1 for x, y in zip(ts, ts[1:]) if y - x > gap)


# ---------------------------------------------------------------- UI helpers
SCORE = []


def ask(n, text, opts):
    print(f"\n>>> Prediction {n}: {text}")
    for k, v in opts.items():
        print(f"    {k}) {v}")
    while True:
        a = input("    Your answer: ").strip().upper()
        if a in opts:
            return a


def grade(n, mine, right, opts):
    ok = mine == right
    SCORE.append((n, mine, right, ok))
    print(f"    You said {mine}. Result matches {right}) {opts[right]}  ->  {'CORRECT' if ok else 'not this time'}")


def pause():
    input("\n    Press Enter for the next phase...")


# ---------------------------------------------------------------- phases
def phase1(assignor, tag):
    lab = Lab(tag)
    extra = {"partition.assignment.strategy": assignor}
    old = [f"c{i}" for i in range(1, 5)]
    for n in old:
        lab.consumer(n, extra)
    lab.wait_stable(old)
    lab.producer()
    time.sleep(5)
    print("  4 consumers own 12 partitions (3 each). Adding consumer c5 now...")
    tj = time.time()
    lab.consumer("c5", extra)
    lab.wait_stable(old + ["c5"])
    time.sleep(4)
    te = time.time()

    revoked = sum(len(e[2]) for e in lab.ev("revoke", tj, old))
    st, stopped = {}, set()
    for kind, name, parts, t in list(lab.events):
        if kind == "assign":
            st.setdefault(name, set()).update(parts)
        elif kind == "revoke":
            st.setdefault(name, set()).difference_update(parts)
            if t >= tj and name in old and not st[name]:
                stopped.add(name)
    gap = lab.longest_gap(tj, te)
    lab.close()
    return {"revoked": revoked, "stopped": len(stopped), "gap": gap}


def phase2():
    lab = Lab("storm")
    extra = {"partition.assignment.strategy": "range",
             "session.timeout.ms": 6000, "max.poll.interval.ms": 7000}
    names = [f"c{i}" for i in range(1, 5)]
    for n in names:
        lab.consumer(n, extra, slow_s=10.0)
    lab.wait_stable(names)
    ts = time.time()
    lab.producer(slow_at=(5, 22))
    print("  Running 45 s. Two slow messages (10 s of work each) arrive at 5 s and 22 s...")
    time.sleep(45)
    te = time.time()
    r = {"slow": len(lab.ev("slow", ts)),
         "kicked": len(lab.ev("kicked", ts)),
         "rebalances": lab.clusters("revoke", ts),
         "dups": lab.duplicates(),
         "idle": lab.idle_total(ts + 1, te)}
    lab.close()
    return r


def phase3(static):
    lab = Lab("static" if static else "dynamic")
    extra = {"partition.assignment.strategy": "range", "session.timeout.ms": 10000}
    names = [f"c{i}" for i in range(1, 5)]

    def iid(n):
        return f"{lab.group}-{n}" if static else None

    lab.consumer("obs", extra)  # observer: never restarted, counts what it suffers
    for n in names:
        lab.consumer(n, extra, iid=iid(n))
    lab.wait_stable(names + ["obs"])
    lab.producer()
    time.sleep(4)
    ts = time.time()
    for n in names:
        print(f"  deploying {n}: stop, wait 3 s, start again...")
        lab.stop(n)
        time.sleep(3)
        lab.consumer(n, extra, iid=iid(n))
        time.sleep(7)
    time.sleep(3)
    te = time.time()

    obs = sorted(lab.ev("revoke", ts, ["obs"]) + lab.ev("assign", ts, ["obs"]), key=lambda e: e[3])
    stopped, t_rev = 0.0, None
    for kind, _n, _p, t in obs:
        if kind == "revoke":
            t_rev = t
        elif t_rev is not None:
            stopped += t - t_rev
            t_rev = None
    r = {"rebalances": len(lab.ev("assign", ts, ["obs"])),
         "obs_stopped": stopped,
         "idle": lab.idle_total(ts, te)}
    lab.close()
    return r


def nearest(value, choices):
    return min(choices, key=lambda kv: abs(kv[1] - value))[0]


# ---------------------------------------------------------------- main
def main():
    if os.environ.get("KAFKA_MODE", "msk") != "local":
        print("Local Docker only. Run:  ./local/lab.sh rebalance_demo.py")
        sys.exit(1)

    print("""
LAB 18: REBALANCE STORMS
A rebalance = the consumer group re-deals partitions among its members.
While a partition is being re-dealt, nobody reads it, so surge goes stale.
We test 3 triggers: a consumer joins, a consumer is too slow, a rolling deploy.
Each phase takes about 30-60 s. Kafka uses 12 partitions, 500 msg/s.""")

    # ---- 1a eager
    o = {"A": "0 partitions", "B": "about 2-3 (only the ones that move to c5)", "C": "all 12"}
    a = ask(1, "EAGER rebalance (range, the classic default). A 5th consumer joins 4. "
               "How many partitions stop being read during the rebalance?", o)
    print("\n  Phase 1a running...")
    e = phase1("range", "eager")
    right = "C" if e["revoked"] >= PARTS else ("A" if e["revoked"] == 0 else "B")
    print(f"\n  RESULT: partitions taken away = {e['revoked']} of 12; "
          f"consumers that fully stopped = {e['stopped']} of 4; "
          f"longest time the WHOLE group read nothing = {e['gap']:.2f} s")
    grade(1, a, right, o)
    print("  WHY: eager = 'everyone hands back everything, then we re-deal'. Even consumers\n"
          "  that keep the same partitions stop. Stop-the-world for the whole group.")
    pause()

    # ---- 1b cooperative
    a = ask(2, "COOPERATIVE rebalance (cooperative-sticky). Same: a 5th consumer joins 4. "
               "How many partitions stop?", o)
    print("\n  Phase 1b running...")
    c = phase1("cooperative-sticky", "coop")
    right = "C" if c["revoked"] >= PARTS else ("A" if c["revoked"] == 0 else "B")
    print(f"\n  RESULT: partitions taken away = {c['revoked']} of 12; "
          f"consumers that fully stopped = {c['stopped']} of 4; "
          f"longest time the WHOLE group read nothing = {c['gap']:.2f} s")
    grade(2, a, right, o)
    print("  WHY: cooperative moves only the partitions that must move (two small rounds).\n"
          "  Everyone else keeps reading. Note: Java's default is still range+cooperative\n"
          "  ('upgrade path'); you must choose cooperative-sticky on purpose.\n"
          "  Kafka 4.x also has a new broker-side protocol (KIP-848) with the same idea.")
    pause()

    # ---- 2 slow consumer
    o2 = {"A": "Nothing special, that partition is just slower",
          "B": "The slow consumer is thrown out, the group re-deals, then it rejoins and the group re-deals AGAIN",
          "C": "The slow message is skipped"}
    a = ask(3, "One message takes 10 s to process, but max.poll.interval.ms is 7 s. What happens?", o2)
    print("\n  Phase 2 running...")
    s = phase2()
    right = "B" if s["kicked"] > 0 else "A"
    print(f"\n  RESULT: slow messages = {s['slow']}; consumers thrown out = {s['kicked']}; "
          f"rebalances = {s['rebalances']}; duplicates = {s['dups']}; "
          f"total time whole group read nothing = {s['idle']:.1f} s")
    grade(3, a, right, o2)
    print("  WHY: heartbeats were fine (they run on a background thread), but the app did\n"
          "  not call poll() in time, so the client left the group. 1 slow message =\n"
          "  about 2 rebalances (leave + rejoin). If the slow work repeats, it becomes a storm.\n"
          "  Duplicates depend on when offsets were last committed (every 5 s here).\n"
          "  Fix: keep slow work out of the poll loop, cap its time, send failures to a\n"
          "  retry/DLQ topic, and size max.poll.interval.ms above the worst real batch.")
    pause()

    # ---- 3a dynamic rolling restart
    o3 = {"A": "0", "B": "about 4", "C": "about 8"}
    a = ask(4, "Rolling deploy: restart 4 consumers one by one (normal membership). "
               "How many rebalances does a 5th consumer that is NOT restarted suffer?", o3)
    print("\n  Phase 3a running (about 50 s)...")
    d = phase3(static=False)
    right = nearest(d["rebalances"], [("A", 0), ("B", 4), ("C", 8)])
    print(f"\n  RESULT: rebalances = {d['rebalances']}; the observer was stopped for "
          f"{d['obs_stopped']:.1f} s; total time whole group read nothing = {d['idle']:.1f} s")
    grade(4, a, right, o3)
    print("  WHY: each restart = 'leave' (rebalance) + 'join' (rebalance). Innocent\n"
          "  consumers pay for every deploy step.")
    pause()

    # ---- 3b static
    o4 = {"A": "About the same as before", "B": "Far fewer, close to 0", "C": "More"}
    a = ask(5, "Same rolling deploy, but each consumer has a fixed group.instance.id "
               "(static membership, session timeout 10 s). Rebalances?", o4)
    print("\n  Phase 3b running (about 50 s)...")
    st = phase3(static=True)
    if st["rebalances"] > d["rebalances"] + 2:
        right = "C"
    elif abs(st["rebalances"] - d["rebalances"]) <= 2:
        right = "A"
    else:
        right = "B"
    print(f"\n  RESULT: rebalances = {st['rebalances']} (was {d['rebalances']}); observer stopped "
          f"{st['obs_stopped']:.1f} s (was {d['obs_stopped']:.1f} s)")
    grade(5, a, right, o4)
    print("  WHY: a static member does not say 'leave' on shutdown. If it comes back with\n"
          "  the same id before session.timeout.ms, it gets its old partitions back with no\n"
          "  re-deal. Trade-off: if it really dies, its partitions wait the FULL session\n"
          "  timeout before anyone else takes them (slower failover).")

    # ---- summary
    got = sum(1 for x in SCORE if x[3])
    print(f"""
================= LAB 18 SUMMARY (paste this in chat) =================
1a eager:       revoked {e['revoked']}/12, stopped {e['stopped']}/4, group gap {e['gap']:.2f}s
1b cooperative: revoked {c['revoked']}/12, stopped {c['stopped']}/4, group gap {c['gap']:.2f}s
2  slow:        slow {s['slow']}, kicked {s['kicked']}, rebalances {s['rebalances']}, dups {s['dups']}, idle {s['idle']:.1f}s
3a dynamic:     rebalances {d['rebalances']}, observer stopped {d['obs_stopped']:.1f}s, idle {d['idle']:.1f}s
3b static:      rebalances {st['rebalances']}, observer stopped {st['obs_stopped']:.1f}s, idle {st['idle']:.1f}s
Predictions: """ + ", ".join(f"Q{n} {m}/{r}{'+' if ok else '-'}" for n, m, r, ok in SCORE) + f"""
Score: {got}/{len(SCORE)}
=======================================================================""")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
