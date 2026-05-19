"""
ingest-service/main.py

Receives log events and alerts, publishes them to Kafka.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_LOGS      = "logs"
TOPIC_ALERTS    = "alerts"

app = FastAPI(title="Incident Ingest Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

producer: Optional[KafkaProducer] = None

def get_producer() -> KafkaProducer:
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            log.info("Kafka producer connected")
        except NoBrokersAvailable:
            raise HTTPException(status_code=503, detail="Kafka not available")
    return producer

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def publish(topic: str, key: str, payload: dict) -> None:
    get_producer().send(topic, key=key, value=payload).get(timeout=5)
    log.info("Published to %s  key=%s", topic, key)


class LogEventIn(BaseModel):
    service:   str
    level:     str = "ERROR"
    message:   str
    host:      str = "unknown"
    timestamp: Optional[int] = None
    labels:    dict[str, str] = Field(default_factory=dict)


class AlertIn(BaseModel):
    name:        str
    source:      str = "custom"
    severity:    str = "HIGH"
    service:     str
    message:     str
    fired_at:    Optional[int] = None
    annotations: dict[str, str] = Field(default_factory=dict)


@app.post("/events/log", status_code=202)
def ingest_log(event: LogEventIn):
    event_id = str(uuid.uuid4())
    payload = {
        "id":        event_id,
        "service":   event.service,
        "level":     event.level.upper(),
        "message":   event.message,
        "host":      event.host,
        "timestamp": event.timestamp or now_ms(),
        "labels":    event.labels,
    }
    publish(TOPIC_LOGS, event_id, payload)
    return {"accepted": True, "event_id": event_id}


@app.post("/events/alert", status_code=202)
def ingest_alert(alert: AlertIn):
    alert_id = str(uuid.uuid4())
    payload = {
        "id":          alert_id,
        "name":        alert.name,
        "source":      alert.source,
        "severity":    alert.severity.upper(),
        "service":     alert.service,
        "message":     alert.message,
        "fired_at":    alert.fired_at or now_ms(),
        "annotations": alert.annotations,
    }
    publish(TOPIC_ALERTS, alert_id, payload)
    return {"accepted": True, "event_id": alert_id}