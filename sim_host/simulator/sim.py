#!/usr/bin/env python3
"""Ride-sharing simulator (Flow 1.A).

Two roles in one process, because they share state:
  - Driver app: each online driver sends a GPS ping every 4s  -> topic gps_pings   (key driver_id)
  - Backend:    ride requests, matching, offers, trip lifecycle -> topic ride_events (key request_id)

Injected faults (for silver dedup / watermark tests):
  - DUP_RATE   same event sent twice (same event_id)
  - LATE_RATE  event held back 1-5 min, then sent with its original event_time
  - drivers going offline / online, rider and driver cancellations, unmatched requests

Settings come from environment variables (see start.sh).
"""
import logging
import math
import os
import random
import signal
import time
import uuid
from collections import Counter

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from kafka_common import REGISTRY_URL, kafka_conf, schema_str

E = os.environ
DRIVERS = int(E.get("DRIVERS", "5000"))
REQ_RATE = float(E.get("REQ_RATE", "4"))           # ride requests per second, all cities
DUP_RATE = float(E.get("DUP_RATE", "0.01"))
LATE_RATE = float(E.get("LATE_RATE", "0.005"))
SEED = int(E.get("SEED", "42"))

PING_EVERY_S = 4.0
TICK_S = 0.2
SPEED_MS = 9.0                 # ~32 km/h in city traffic
ARRIVE_M = 40                  # "at the pickup / dropoff" radius
MATCH_RADIUS_M = 4000
MAX_OFFERS = 3
SEARCH_TIMEOUT_S = 90

# code, country, centre lat, centre lon, share of drivers/demand
CITIES = [
    ("NYC", "US", 40.7128, -74.0060, 0.20),
    ("SFO", "US", 37.7749, -122.4194, 0.10),
    ("BER", "DE", 52.5200, 13.4050, 0.15),
    ("MUC", "DE", 48.1351, 11.5820, 0.10),
    ("BLR", "IN", 12.9716, 77.5946, 0.25),
    ("BOM", "IN", 19.0760, 72.8777, 0.20),
]
HALF_LAT = 0.08                # city box ~18 km tall
PAYMENT_MIX = {
    "US": [("CARD", 0.85), ("WALLET", 0.15)],
    "DE": [("CARD", 0.70), ("WALLET", 0.20), ("CASH", 0.10)],
    "IN": [("CASH", 0.40), ("WALLET", 0.40), ("CARD", 0.20)],
}
RIDE_OPTIONAL = ["rider_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
                 "payment_method", "surge_multiplier_shown", "driver_id", "trip_id",
                 "cancelled_by", "cancel_reason"]

log = logging.getLogger("rs-sim")
rnd = random.Random(SEED)


def now_ms():
    return int(time.time() * 1000)


def dist_m(lat1, lon1, lat2, lon2):
    x = (lon2 - lon1) * 111320 * math.cos(math.radians((lat1 + lat2) / 2))
    y = (lat2 - lat1) * 110540
    return math.hypot(x, y)


def step_toward(lat, lon, tlat, tlon, meters):
    d = dist_m(lat, lon, tlat, tlon)
    if d <= meters:
        return tlat, tlon
    f = meters / d
    return lat + (tlat - lat) * f, lon + (tlon - lon) * f


def weighted(pairs):
    r, acc = rnd.random(), 0.0
    for value, w in pairs:
        acc += w
        if r <= acc:
            return value
    return pairs[-1][0]


class City:
    def __init__(self, idx, code, country, lat, lon, share):
        self.idx, self.code, self.country = idx, code, country
        self.lat, self.lon, self.share = lat, lon, share
        self.half_lon = HALF_LAT / math.cos(math.radians(lat))

    def random_point(self):
        return (self.lat + rnd.uniform(-HALF_LAT, HALF_LAT),
                self.lon + rnd.uniform(-self.half_lon, self.half_lon))

    def inside(self, lat, lon):
        return abs(lat - self.lat) <= HALF_LAT and abs(lon - self.lon) <= self.half_lon


class Driver:
    def __init__(self, driver_id, city, start):
        self.id, self.city = driver_id, city
        self.lat, self.lon = city.random_point()
        self.heading = rnd.uniform(0, 2 * math.pi)
        self.online = rnd.random() < 0.85
        self.status = "AVAILABLE" if self.online else "OFFLINE"
        self.ride = None          # Ride while en route / on trip
        self.reserved = False     # holding an open offer
        self.next_ping = start + rnd.uniform(0, PING_EVERY_S if self.online else 300)

    def free(self):
        return self.online and self.status == "AVAILABLE" and not self.reserved


class Ride:
    def __init__(self, city, t):
        self.request_id = str(uuid.uuid4())
        self.city = city
        self.rider_id = city.idx * 10_000_000 + rnd.randint(1, 2_000_000)
        self.pickup = city.random_point()
        while True:
            self.dropoff = city.random_point()
            if 1500 <= dist_m(*self.pickup, *self.dropoff) <= 8000:
                break
        self.payment = weighted(PAYMENT_MIX[city.country])
        self.state = "SEARCHING"
        self.created = t
        self.deadline = None
        self.driver = None
        self.trip_id = None
        self.offers = 0
        self.tried = set()
        self.offer_outcome = None
        # pre-decided behaviour (keeps the main loop simple)
        self.rider_cancel_at = t + rnd.uniform(5, 40) if rnd.random() < 0.03 else None
        self.cancel_after_accept = weighted([(None, 0.93), ("RIDER", 0.05), ("DRIVER", 0.02)])
        self.no_show = rnd.random() < 0.02


class Simulator:
    def __init__(self):
        self.running = True
        self.stats = Counter()
        self.late = []                       # (release_time, topic, key, value)
        self.rides = {}
        self.trip_seq = int(time.time()) * 1000
        t = time.time()
        self.next_request = t + rnd.expovariate(REQ_RATE) if REQ_RATE > 0 else float("inf")

        self.cities = [City(i, *c) for i, c in enumerate(CITIES)]
        self.drivers, self.by_city = [], {c.idx: [] for c in self.cities}
        for c in self.cities:
            n = max(1, round(DRIVERS * c.share))
            for i in range(1, n + 1):
                d = Driver(c.idx * 1_000_000 + i, c, t)
                self.drivers.append(d)
                self.by_city[c.idx].append(d)

        sr = SchemaRegistryClient({"url": REGISTRY_URL})
        ser_conf = {"auto.register.schemas": False}    # schemas are registered from the repo only
        self.ser = {
            "gps_pings": AvroSerializer(sr, schema_str("gps_pings-value"), conf=ser_conf),
            "ride_events": AvroSerializer(sr, schema_str("ride_events-value"), conf=ser_conf),
        }
        self.producer = Producer(kafka_conf(**{
            "client.id": "rs-sim",
            "acks": "all",                    # with min.insync=2: no acked-message loss
            "enable.idempotence": True,       # broker drops producer retries' duplicates
            "linger.ms": 50,
            "compression.type": "lz4",
            "partitioner": "murmur2_random",  # same key -> same partition as Java producers
        }))
        self.producer.poll(1)                 # triggers the first IAM token
        log.info("drivers=%d (online=%d) req_rate=%.2f/s dup=%.3f late=%.3f",
                 len(self.drivers), sum(d.online for d in self.drivers), REQ_RATE, DUP_RATE, LATE_RATE)

    # ---------- Kafka ----------
    def _delivered(self, err, _msg):
        self.stats["delivery_errors" if err else "delivered"] += 1
        if err:
            log.warning("delivery failed: %s", err)

    def _produce(self, topic, key, value):
        data = self.ser[topic](value, SerializationContext(topic, MessageField.VALUE))
        while True:
            try:
                self.producer.produce(topic, key=str(key).encode(), value=data, on_delivery=self._delivered)
                break
            except BufferError:
                self.producer.poll(0.5)
        self.stats[topic] += 1

    def send(self, topic, key, value, t):
        if rnd.random() < LATE_RATE:
            self.late.append((t + rnd.uniform(60, 300), topic, key, value))
            self.stats["late_held"] += 1
            return
        self._produce(topic, key, value)
        if rnd.random() < DUP_RATE:
            self._produce(topic, key, value)
            self.stats["dup_sent"] += 1

    def release_late(self, t):
        due = [x for x in self.late if x[0] <= t]
        if due:
            self.late = [x for x in self.late if x[0] > t]
            for _, topic, key, value in due:
                self._produce(topic, key, value)
                self.stats["late_sent"] += 1

    # ---------- events ----------
    def ping(self, d, t):
        self.send("gps_pings", d.id, {
            "event_id": str(uuid.uuid4()), "driver_id": d.id, "event_time": now_ms(),
            "driver_status": d.status,
            "trip_id": d.ride.trip_id if d.ride and d.status in ("EN_ROUTE_TO_PICKUP", "ON_TRIP") else None,
            "lat": round(d.lat, 6), "lon": round(d.lon, 6),
        }, t)

    def ride_event(self, etype, ride, t, **extra):
        v = {k: None for k in RIDE_OPTIONAL}
        v.update(event_id=str(uuid.uuid4()), event_type=etype,
                 request_id=ride.request_id, event_time=now_ms())
        v.update(extra)
        self.send("ride_events", ride.request_id, v, t)
        self.stats["ev_" + etype] += 1

    # ---------- driver app ----------
    def move(self, d):
        dt = PING_EVERY_S
        if d.status == "EN_ROUTE_TO_PICKUP":
            d.lat, d.lon = step_toward(d.lat, d.lon, *d.ride.pickup, SPEED_MS * dt)
        elif d.status == "ON_TRIP":
            d.lat, d.lon = step_toward(d.lat, d.lon, *d.ride.dropoff, SPEED_MS * dt)
        elif d.status == "AVAILABLE":
            d.heading += rnd.gauss(0, 0.3)
            m = 0.5 * SPEED_MS * dt
            nlat = d.lat + math.cos(d.heading) * m / 110540
            nlon = d.lon + math.sin(d.heading) * m / (111320 * math.cos(math.radians(d.lat)))
            if d.city.inside(nlat, nlon):
                d.lat, d.lon = nlat, nlon
            else:
                d.heading += math.pi

    def driver_tick(self, d, t):
        if not d.online:                                   # coming back online
            d.online, d.status = True, "AVAILABLE"
            d.next_ping = t + PING_EVERY_S
            self.ping(d, t)
            return
        self.move(d)
        r = d.ride
        if r and r.state == "ACCEPTED" and dist_m(d.lat, d.lon, *r.pickup) <= ARRIVE_M:
            r.state, r.deadline = "ARRIVED", t + rnd.uniform(20, 90)
            self.ride_event("DRIVER_ARRIVED", r, t, driver_id=d.id, trip_id=r.trip_id)
        elif r and d.status == "ON_TRIP" and dist_m(d.lat, d.lon, *r.dropoff) <= ARRIVE_M:
            self.ride_event("TRIP_COMPLETED", r, t, driver_id=d.id, trip_id=r.trip_id)
            self.finish(r)
        elif d.free() and rnd.random() < 0.01 * PING_EVERY_S / 60:   # ~1%/min goes offline
            d.status, d.online = "OFFLINE", False
            self.ping(d, t)
            d.next_ping = t + rnd.uniform(60, 600)
            return
        self.ping(d, t)
        d.next_ping += PING_EVERY_S

    # ---------- backend ----------
    def new_request(self, t):
        city = rnd.choices(self.cities, weights=[c.share for c in self.cities])[0]
        r = Ride(city, t)
        self.rides[r.request_id] = r
        self.ride_event("RIDE_REQUESTED", r, t, rider_id=r.rider_id,
                        pickup_lat=round(r.pickup[0], 6), pickup_lon=round(r.pickup[1], 6),
                        dropoff_lat=round(r.dropoff[0], 6), dropoff_lon=round(r.dropoff[1], 6),
                        payment_method=r.payment, surge_multiplier_shown=1.0)

    def nearest_driver(self, r):
        best, best_d = None, MATCH_RADIUS_M
        for d in self.by_city[r.city.idx]:
            if d.free() and d.id not in r.tried:
                dd = dist_m(d.lat, d.lon, *r.pickup)
                if dd < best_d:
                    best, best_d = d, dd
        return best

    def cancel(self, r, t, by, reason):
        self.ride_event("RIDE_CANCELLED", r, t, cancelled_by=by, cancel_reason=reason,
                        driver_id=r.driver.id if r.driver else None, trip_id=r.trip_id)
        self.finish(r)

    def finish(self, r):
        d = r.driver
        if d:
            d.ride, d.reserved = None, False
            if d.online:
                d.status = "AVAILABLE"
        self.rides.pop(r.request_id, None)

    def ride_tick(self, r, t):
        if r.state == "SEARCHING":
            if r.rider_cancel_at and t >= r.rider_cancel_at:
                return self.cancel(r, t, "RIDER", "CHANGED_MIND")
            if t - r.created > SEARCH_TIMEOUT_S:
                return self.cancel(r, t, "SYSTEM", "NO_DRIVER_FOUND")
            d = self.nearest_driver(r)
            if d:
                d.reserved = True
                r.driver, r.offers, r.state = d, r.offers + 1, "OFFERED"
                r.tried.add(d.id)
                r.offer_outcome = weighted([("ACCEPT", 0.75), ("REJECT", 0.15), ("EXPIRE", 0.10)])
                r.deadline = t + (15 if r.offer_outcome == "EXPIRE" else rnd.uniform(3, 10))
                self.ride_event("OFFER_SENT", r, t, driver_id=d.id)

        elif r.state == "OFFERED" and t >= r.deadline:
            d = r.driver
            if r.offer_outcome == "ACCEPT":
                self.trip_seq += 1
                r.trip_id, r.state = self.trip_seq, "ACCEPTED"
                d.reserved, d.ride, d.status = False, r, "EN_ROUTE_TO_PICKUP"
                r.deadline = t + rnd.uniform(20, 120)     # when a planned cancel would happen
                self.ride_event("RIDE_ACCEPTED", r, t, driver_id=d.id, trip_id=r.trip_id)
            else:
                etype = "OFFER_REJECTED" if r.offer_outcome == "REJECT" else "OFFER_EXPIRED"
                self.ride_event(etype, r, t, driver_id=d.id)
                d.reserved, r.driver = False, None
                if r.offers >= MAX_OFFERS:
                    self.cancel(r, t, "SYSTEM", "NO_DRIVER_ACCEPTED")
                else:
                    r.state = "SEARCHING"

        elif r.state == "ACCEPTED" and r.cancel_after_accept and t >= r.deadline:
            reason = "CHANGED_MIND" if r.cancel_after_accept == "RIDER" else "DRIVER_CANCELLED"
            self.cancel(r, t, r.cancel_after_accept, reason)

        elif r.state == "ARRIVED" and t >= r.deadline:
            if r.no_show:
                return self.cancel(r, t, "DRIVER", "RIDER_NO_SHOW")
            r.state, r.driver.status = "ON_TRIP", "ON_TRIP"
            self.ride_event("TRIP_STARTED", r, t, driver_id=r.driver.id, trip_id=r.trip_id)

    # ---------- main loop ----------
    def log_stats(self, interval):
        s = self.stats
        st = Counter(d.status for d in self.drivers)
        rs = Counter(r.state for r in self.rides.values())
        log.info("pings/s=%.0f ride_ev/s=%.1f delivered=%d errors=%d dup=%d late_held=%d late_sent=%d "
                 "pending_late=%d | drivers %s | rides %s",
                 s["gps_pings"] / interval, s["ride_events"] / interval, s["delivered"],
                 s["delivery_errors"], s["dup_sent"], s["late_held"], s["late_sent"],
                 len(self.late), dict(st), dict(rs))
        for k in ("gps_pings", "ride_events", "delivered", "delivery_errors",
                  "dup_sent", "late_held", "late_sent"):
            s[k] = 0

    def run(self):
        last_stats = time.time()
        while self.running:
            t = time.time()
            while t >= self.next_request:
                self.new_request(t)
                self.next_request += rnd.expovariate(REQ_RATE)
            for r in list(self.rides.values()):
                self.ride_tick(r, t)
            for d in self.drivers:
                if t >= d.next_ping:
                    self.driver_tick(d, t)
            self.release_late(t)
            self.producer.poll(0)
            if t - last_stats >= 10:
                self.log_stats(t - last_stats)
                last_stats = t
            time.sleep(max(0.0, TICK_S - (time.time() - t)))
        log.info("stopping: flushing producer")
        left = self.producer.flush(15)
        log.info("stopped (undelivered=%d, late events dropped=%d)", left, len(self.late))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sim = Simulator()

    def stop(*_):
        sim.running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    sim.run()


if __name__ == "__main__":
    main()
