#!/usr/bin/env python3
"""Lab 8: serialisation, and a message that won't decode.
Part A - anatomy: the newest gps_pings message as raw bytes (magic byte + schema ID + Avro),
         its schema fetched from the registry by ID, and its size vs the same ping as JSON.
Part B - poison message: on a separate test topic (lab_serde, 1 partition) write
         good, good, JSON, good, unknown schema ID, truncated Avro, good.
         A naive consumer (commit after each good message, crash on error) is restarted 3 times.
         A tolerant consumer sets bad messages aside and carries on.
gps_pings is never written to.
Usage: serde_demo.py"""
import io
import json
import os
import sys
import time
import uuid

import urllib.request

import fastavro
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import REGISTRY_URL, kafka_conf, schema_str  # noqa: E402

de = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY_URL}))

# ---------------- Part A: anatomy of one message ----------------
print("== Part A: one gps_pings message, byte by byte")
c = Consumer(kafka_conf(**{"group.id": "lab8-peek", "enable.auto.commit": False}))
lo, hi = c.get_watermark_offsets(TopicPartition("gps_pings", 0), timeout=15)
c.assign([TopicPartition("gps_pings", 0, hi - 1)])
m = None
while m is None or m.error():
    m = c.poll(5.0)
c.close()
raw = m.value()
schema_id = int.from_bytes(raw[1:5], "big")
print(f"total value bytes : {len(raw)}")
print(f"first 12 bytes hex: {raw[:12].hex(' ')}")
print(f"  byte 0          : {raw[0]}  (magic byte: 0 = Confluent wire format)")
print(f"  bytes 1-4       : {raw[1:5].hex(' ')} = schema ID {schema_id}")
print(f"  bytes 5..end    : {len(raw) - 5} bytes of Avro data (no field names inside)")
with urllib.request.urlopen(f"{REGISTRY_URL}/schemas/ids/{schema_id}", timeout=10) as r:
    sch = json.loads(r.read())["schema"]
print(f"registry: schema ID {schema_id} -> record '{json.loads(sch)['name']}' "
      f"with fields {[f['name'] for f in json.loads(sch)['fields']]}")
rec = de(raw, SerializationContext("gps_pings", MessageField.VALUE))
as_json = json.dumps({**rec, "event_time": int(rec["event_time"].timestamp() * 1000)}, default=str)
print(f"decoded: {as_json}")
print(f"same ping as JSON: {len(as_json.encode())} bytes vs Avro {len(raw)} bytes "
      f"(x{len(as_json.encode()) / len(raw):.1f})")

# ---------------- Part B: a poison message ----------------
print("\n== Part B: a message that won't decode")
TOPIC = "lab_serde"
admin = AdminClient(kafka_conf())
if TOPIC in admin.list_topics(timeout=15).topics:
    admin.delete_topics([TOPIC])[TOPIC].result(30)
    time.sleep(5)
admin.create_topics([NewTopic(TOPIC, 1, 3, config={"min.insync.replicas": "2"})])[TOPIC].result(30)
time.sleep(5)

parsed = fastavro.parse_schema(json.loads(schema_str("gps_pings-value")))


def avro(driver):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, {
        "event_id": str(uuid.uuid4()), "driver_id": driver, "event_time": int(time.time() * 1000),
        "driver_status": "AVAILABLE", "trip_id": None, "lat": 12.97, "lon": 77.59})
    return b"\x00" + (1).to_bytes(4, "big") + buf.getvalue()


good = avro(1)
plan = [
    ("good", avro(1)),
    ("good", avro(2)),
    ("plain JSON", json.dumps({"driver_id": 3, "lat": 12.97, "lon": 77.59}).encode()),
    ("good", avro(4)),
    ("unknown schema ID 999", b"\x00" + (999).to_bytes(4, "big") + good[5:]),
    ("truncated Avro", good[:20]),
    ("good", avro(7)),
]
p = Producer(kafka_conf(**{"acks": "all"}))
for i, (kind, v) in enumerate(plan):
    p.produce(TOPIC, key=str(i).encode(), value=v)
p.flush(30)
print("written to lab_serde: " + ", ".join(f"{i}={k}" for i, (k, _) in enumerate(plan)))


def consume(group, tolerant):
    cons = Consumer(kafka_conf(**{"group.id": group, "enable.auto.commit": False,
                                  "auto.offset.reset": "earliest"}))
    cons.assign([TopicPartition(TOPIC, 0)])
    ok, bad = [], []
    end = time.time() + 15
    while time.time() < end:
        msg = cons.poll(2.0)
        if msg is None:
            if len(ok) + len(bad) >= len(plan) or (ok or bad):
                break
            continue
        if msg.error():
            continue
        try:
            de(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
            ok.append(msg.offset())
        except Exception as e:            # noqa: BLE001
            err = str(e).splitlines()[0][:90]
            if not tolerant:
                cons.close()
                return ok, msg.offset(), err
            bad.append((msg.offset(), err))
        cons.commit(offsets=[TopicPartition(TOPIC, 0, msg.offset() + 1)], asynchronous=False)
    cons.close()
    return ok, bad, None


naive = f"lab8-naive-{uuid.uuid4().hex[:6]}"
print(f"\nnaive consumer (group {naive}): commits after each good message, crashes on an error")
for restart in range(1, 4):
    ok, stuck_at, err = consume(naive, tolerant=False)
    print(f"  run {restart}: processed offsets {ok or 'none'}, CRASHED at offset {stuck_at}: {err}")

tol = f"lab8-tolerant-{uuid.uuid4().hex[:6]}"
ok, bad, _ = consume(tol, tolerant=True)
print(f"\ntolerant consumer (group {tol}): sets bad messages aside and carries on")
print(f"  processed offsets {ok}")
for off, err in bad:
    print(f"  set aside offset {off}: {err}")
