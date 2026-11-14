#!/usr/bin/env python3
"""Lab 3: keys and partitions.
Reads the newest N messages per partition and checks:
  1. does each key (driver_id) live in exactly one partition?
  2. which hash put it there? librdkafka CRC32 (our Python producer) vs Java's murmur2
  3. per key, does event_time always go up with the offset?
Usage: keys_check.py [topic=gps_pings] [per_partition=20000] [last_minutes=0 (0 = all read)]"""
import sys
import time
import zlib
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

sys.path.insert(0, "/opt/rs/sim_host/simulator")
from kafka_common import REGISTRY_URL, kafka_conf  # noqa: E402


def murmur2(data: bytes) -> int:
    """Kafka's Java default partitioner hash (Utils.murmur2)."""
    length, seed, m, r = len(data), 0x9747B28C, 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[i * 4:i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    extra, idx = length % 4, length & ~3
    if extra == 3:
        h ^= data[idx + 2] << 16
    if extra >= 2:
        h ^= data[idx + 1] << 8
    if extra >= 1:
        h ^= data[idx]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


topic = sys.argv[1] if len(sys.argv) > 1 else "gps_pings"
per_part = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
last_min = float(sys.argv[3]) if len(sys.argv) > 3 else 0
since_ms = int(time.time() * 1000 - last_min * 60_000) if last_min else 0

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
key_last_time = {}
backwards, backwards_keys, total = 0, set(), 0
done = set()
while len(done) < n:
    m = c.poll(5.0)
    if m is None:
        break
    if m.error():
        continue
    if m.offset() >= ends[m.partition()] - 1:
        done.add(m.partition())
    if m.timestamp()[1] < since_ms:
        continue
    key = m.key()
    v = de(m.value(), SerializationContext(topic, MessageField.VALUE))
    key_parts[key].add(m.partition())
    t = v["event_time"]
    if key in key_last_time and t < key_last_time[key]:
        backwards += 1
        backwards_keys.add(key)
    key_last_time[key] = max(t, key_last_time.get(key, t))
    total += 1
c.close()

multi = [k for k, ps in key_parts.items() if len(ps) > 1]
crc_match = sum(1 for k, ps in key_parts.items() if (zlib.crc32(k) % n) in ps)
java_same = sum(1 for k, ps in key_parts.items() if ((murmur2(k) & 0x7FFFFFFF) % n) in ps)
keys_per_part = defaultdict(int)
for ps in key_parts.values():
    for p in ps:
        keys_per_part[p] += 1

window = f"last {last_min:g} min" if last_min else "all read"
print(f"topic={topic} counted={total} ({window}) distinct keys={len(key_parts)} partitions={n}")
print(f"1. keys found on more than one partition: {len(multi)}")
print(f"   keys per partition: {dict(sorted(keys_per_part.items()))}")
print(f"2. partition == CRC32(key) % {n} (librdkafka default): {crc_match} of {len(key_parts)} keys")
print(f"   a Java producer (murmur2) would pick the SAME partition for: {java_same} of {len(key_parts)} keys "
      f"({100 * java_same / max(len(key_parts), 1):.0f}%)")
print(f"3. per-key event_time going BACKWARDS as offset rises: {backwards} messages, {len(backwards_keys)} keys")
if not key_parts:
    sys.exit("no messages in the chosen window")
ex = next(iter(key_parts))
print(f"   example key {ex.decode()}: partition {sorted(key_parts[ex])}, "
      f"crc32->{zlib.crc32(ex) % n}, murmur2->{(murmur2(ex) & 0x7FFFFFFF) % n}")
