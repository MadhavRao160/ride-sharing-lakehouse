#!/usr/bin/env python3
"""Read the newest N messages from a topic, decode with the registry, print them.
Usage: peek.py <topic> [n=3] [timeout_s=20]"""
import json
import sys
import time
import uuid

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from kafka_common import REGISTRY_URL, kafka_conf

topic = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
timeout = float(sys.argv[3]) if len(sys.argv) > 3 else 20

de = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY_URL}))
c = Consumer(kafka_conf(**{"group.id": "rs-peek-" + uuid.uuid4().hex[:8],
                           "auto.offset.reset": "latest", "enable.auto.commit": False}))
c.subscribe([topic])
got, end = 0, time.time() + timeout
while got < n and time.time() < end:
    m = c.poll(1.0)
    if m is None:
        continue
    if m.error():
        print("error:", m.error())
        continue
    v = de(m.value(), SerializationContext(topic, MessageField.VALUE))
    schema_id = int.from_bytes(m.value()[1:5], "big")
    print(f"[{topic} p{m.partition()} o{m.offset()} key={m.key().decode()} schema_id={schema_id}] "
          + json.dumps(v, default=str))
    got += 1
c.close()
if got == 0:
    print(f"no messages on {topic} within {timeout:.0f}s")
    sys.exit(1)
