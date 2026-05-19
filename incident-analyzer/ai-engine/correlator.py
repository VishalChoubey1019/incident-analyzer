"""
Temporary stand-in for the Flink job.
Reads from logs + alerts topics, groups errors by service,
writes correlated incidents to the incidents topic.
"""
import json, time, uuid
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"

consumer = KafkaConsumer(
    "logs", "alerts",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="python-correlator",
    value_deserializer=lambda b: json.loads(b.decode()),
    auto_offset_reset="earliest",
    consumer_timeout_ms=1000,
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode(),
)

# buffer: service -> list of error events in current window
buffers = defaultdict(list)
last_flush = time.time()
WINDOW_SECS = 15

def flush():
    for service, events in buffers.items():
        if len(events) >= 2:  # lowered threshold for demo
            incident = {
                "id":              f"inc-{uuid.uuid4()}",
                "title":           f"{service} — {len(events)} errors",
                "description":     "; ".join(e.get("message","")[:80] for e in events[:3]),
                "severity":        "CRITICAL" if len(events) >= 6 else "HIGH" if len(events) >= 3 else "MEDIUM",
                "status":          "OPEN",
                "service":         service,
                "startedAt":       int(datetime.now(timezone.utc).timestamp() * 1000),
                "relatedEventIds": [e.get("id","") for e in events],
                "source":          "log-correlation",
            }
            producer.send("incidents", value=incident)
            print(f"  → incident emitted for {service} ({len(events)} events)")
    buffers.clear()

print("Correlator running — waiting for events ...")
while True:
    for msg in consumer:
        data = msg.value
        if msg.topic == "alerts":
            incident = {
                "id":              f"inc-{uuid.uuid4()}",
                "title":           data.get("name", "Alert"),
                "description":     data.get("message", ""),
                "severity":        data.get("severity", "HIGH"),
                "status":          "OPEN",
                "service":         data.get("service", "unknown"),
                "startedAt":       int(datetime.now(timezone.utc).timestamp() * 1000),
                "relatedEventIds": [data.get("id","")],
                "source":          "alert",
            }
            producer.send("incidents", value=incident)
            print(f"  → alert incident: {incident['title']}")
        elif msg.topic == "logs" and data.get("level") == "ERROR":
            buffers[data.get("service","unknown")].append(data)

    if time.time() - last_flush > WINDOW_SECS:
        flush()
        last_flush = time.time()
