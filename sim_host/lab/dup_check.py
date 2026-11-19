#!/usr/bin/env python3
"""Lab 2: duplicates in Kafka.
Reads the newest N messages from every partition of a topic, decodes them,
and finds event_ids that appear more than once.
Usage: dup_check.py [topic=gps_pings] [per_partition=20000]"""
import os
import sys
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import REGISTRY_URL, kafka_conf  # noqa: E402

topic = sys.argv[1] if len(sys.argv) > 1 else "gps_pings"
per_part = int(sys.argv[2]) if len(sys.argv) > 2 else 20000

de = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY_URL}))
c = Consumer(kafka_conf(**{"group.id": "rs-lab-dup", "enable.auto.commit": False}))
parts = c.list_topics(topic, timeout=15).topics[topic].partitions

assign, ends = [], {}
for p in parts:
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
    start = max(lo, hi - per_part)
    assign.append(TopicPartition(topic, p, start))
    ends[p] = hi
c.assign(assign)

seen = defaultdict(list)          # event_id -> [(partition, offset, broker_ts_ms)]
done, total = set(), 0
while len(done) < len(parts):
    m = c.poll(5.0)
    if m is None:
        break
    if m.error():
        continue
    v = de(m.value(), SerializationContext(topic, MessageField.VALUE))
    seen[str(v["event_id"])].append((m.partition(), m.offset(), m.timestamp()[1]))
    total += 1
    if m.offset() >= ends[m.partition()] - 1:
        done.add(m.partition())
c.close()

dups = {k: v for k, v in seen.items() if len(v) > 1}
same_part = sum(1 for v in dups.values() if len({x[0] for x in v}) == 1)
gaps = sorted(v[1][1] - v[0][1] for v in dups.values())
print(f"topic={topic} read={total} unique_event_ids={len(seen)}")
print(f"duplicated event_ids={len(dups)} ({100 * len(dups) / max(total, 1):.2f}% of messages)")
print(f"copies in the SAME partition: {same_part} of {len(dups)}")
if gaps:
    print(f"offset gap between copies: min={gaps[0]} median={gaps[len(gaps) // 2]} max={gaps[-1]}")
print("examples (partition, offset, broker_time_ms):")
for k, v in list(dups.items())[:3]:
    print(f"  {k}: {v}")
