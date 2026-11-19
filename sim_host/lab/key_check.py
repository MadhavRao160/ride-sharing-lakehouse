#!/usr/bin/env python3
"""Lab 3: keys and partitions.
Reads the newest N messages per partition and checks:
  1. Does every key live in exactly ONE partition?
  2. Which hash put it there? (librdkafka CRC32 vs Java murmur2)
  3. Within one key, does event_time always increase in offset order?
Usage: key_check.py [topic=gps_pings] [per_partition=20000]"""
import os
import sys
import zlib
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "simulator"))
from kafka_common import REGISTRY_URL, kafka_conf  # noqa: E402


def murmur2(data: bytes) -> int:
    """Kafka Java client's default key hash (Utils.murmur2)."""
    length, seed, m, r = len(data), 0x9747B28C, 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[i * 4:i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail, extra = length & ~3, length % 4
    if extra == 3:
        h ^= data[tail + 2] << 16
    if extra >= 2:
        h ^= data[tail + 1] << 8
    if extra >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def java_partition(key: bytes, n: int) -> int:
    return (murmur2(key) & 0x7FFFFFFF) % n


def librdkafka_partition(key: bytes, n: int) -> int:
    return (zlib.crc32(key) & 0xFFFFFFFF) % n


topic = sys.argv[1] if len(sys.argv) > 1 else "gps_pings"
per_part = int(sys.argv[2]) if len(sys.argv) > 2 else 20000

de = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY_URL}))
c = Consumer(kafka_conf(**{"group.id": "rs-lab-keys", "enable.auto.commit": False}))
parts = c.list_topics(topic, timeout=15).topics[topic].partitions
n = len(parts)

assign, ends = [], {}
for p in parts:
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
    assign.append(TopicPartition(topic, p, max(lo, hi - per_part)))
    ends[p] = hi
c.assign(assign)

key_parts = defaultdict(set)
last_time = {}                 # key -> last event_time seen (offset order)
out_of_order, total, done = defaultdict(int), 0, set()
while len(done) < n:
    m = c.poll(5.0)
    if m is None:
        break
    if m.error():
        continue
    key = m.key()
    v = de(m.value(), SerializationContext(topic, MessageField.VALUE))
    et = v["event_time"]
    key_parts[key].add(m.partition())
    if key in last_time and et < last_time[key]:
        out_of_order[key] += 1
    last_time[key] = max(et, last_time.get(key, et))
    total += 1
    if m.offset() >= ends[m.partition()] - 1:
        done.add(m.partition())
c.close()

keys = list(key_parts)
multi = [k for k in keys if len(key_parts[k]) > 1]
match_rd = sum(1 for k in keys if librdkafka_partition(k, n) in key_parts[k])
match_java = sum(1 for k in keys if java_partition(k, n) in key_parts[k])
per_part_keys = defaultdict(int)
for k in keys:
    per_part_keys[next(iter(key_parts[k]))] += 1

print(f"topic={topic} messages={total} distinct keys={len(keys)} partitions={n}")
print(f"1. keys found in MORE than one partition: {len(multi)}")
print(f"   distinct keys per partition: {dict(sorted(per_part_keys.items()))}")
print(f"2. partition explained by librdkafka CRC32 hash: {match_rd}/{len(keys)}")
print(f"   partition a Java producer (murmur2) would pick, same as actual: {match_java}/{len(keys)}"
      f"  -> {100 * (len(keys) - match_java) / max(len(keys), 1):.0f}% of keys would MOVE")
sample = keys[:3]
for k in sample:
    print(f"   key {k.decode():>8}: actual p{sorted(key_parts[k])}  crc32->p{librdkafka_partition(k, n)}"
          f"  murmur2->p{java_partition(k, n)}")
bad = sum(out_of_order.values())
print(f"3. pings arriving with an OLDER event_time than the previous one for the same key: {bad}"
      f" ({100 * bad / max(total, 1):.2f}%), across {len(out_of_order)} keys")
