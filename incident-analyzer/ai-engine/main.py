"""
ai-engine/main.py

Consumes correlated incidents from Kafka, calls the local Llama 3 model
via Ollama to generate a root cause analysis, then upserts the enriched
incident into MongoDB.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone

import httpx
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("ai-engine")

KAFKA_BOOTSTRAP  = "localhost:9092"
KAFKA_TOPIC      = "incidents"
KAFKA_GROUP      = "ai-engine"
MONGO_URI        = "mongodb://admin:incident123@localhost:27017"
MONGO_DB         = "incidents"
OLLAMA_URL       = "http://localhost:11434/api/generate"
OLLAMA_MODEL     = "llama3"


# ── Prompt template ───────────────────────────────────────────────────────

RCA_PROMPT = """You are a senior site reliability engineer analysing a production incident.
Given the following incident details, provide a concise root cause analysis.

Service: {service}
Severity: {severity}
Title: {title}
Description: {description}

Respond in this exact JSON format (no markdown, no extra text):
{{
  "summary": "one sentence describing what happened",
  "likely_cause": "most probable root cause in one or two sentences",
  "recommendations": ["action 1", "action 2", "action 3"],
  "confidence_score": 0.0
}}

Keep each recommendation under 15 words. Be direct and technical."""


# ── Ollama client ─────────────────────────────────────────────────────────

def call_ollama(prompt: str, retries: int = 2) -> dict:
    """Call local Ollama and return parsed JSON from the model response."""
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.3,   # low temp → more deterministic RCA
            "num_predict": 300,
        },
    }

    for attempt in range(retries + 1):
        try:
            resp = httpx.post(OLLAMA_URL, json=payload, timeout=60.0)
            resp.raise_for_status()
            raw_text = resp.json()["response"].strip()

            # strip any accidental markdown fences the model adds
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]

            return json.loads(raw_text.strip())

        except (httpx.HTTPError, json.JSONDecodeError) as e:
            log.warning("Ollama call failed (attempt %d/%d): %s", attempt + 1, retries + 1, e)
            if attempt < retries:
                time.sleep(2 ** attempt)

    # fallback if all attempts fail
    return {
        "summary":         "Analysis unavailable — model did not respond.",
        "likely_cause":    "Unknown",
        "recommendations": ["Check service logs manually", "Review recent deployments"],
        "confidence_score": 0.0,
    }


# ── MongoDB helpers ───────────────────────────────────────────────────────

def get_mongo_collection(db_name: str, collection: str):
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client[db_name][collection]


def upsert_incident(incident: dict) -> None:
    col = get_mongo_collection(MONGO_DB, "incidents")
    col.update_one(
        {"id": incident["id"]},
        {"$set": incident},
        upsert=True,
    )
    log.info("Upserted incident id=%s service=%s", incident["id"], incident.get("service"))


# ── Main processing loop ──────────────────────────────────────────────────

def process_incident(raw: dict) -> None:
    incident_id = raw.get("id") or str(uuid.uuid4())
    service     = raw.get("service", "unknown")
    severity    = raw.get("severity", "MEDIUM")
    title       = raw.get("title", "Unnamed incident")
    description = raw.get("description", "")

    log.info("Processing incident id=%s service=%s severity=%s", incident_id, service, severity)

    prompt = RCA_PROMPT.format(
        service=service,
        severity=severity,
        title=title,
        description=description,
    )

    rca_raw = call_ollama(prompt)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    enriched = {
        **raw,
        "id":       incident_id,
        "status":   raw.get("status", "OPEN"),
        "updatedAt": now_ms,
        "rca": {
            "summary":          rca_raw.get("summary", ""),
            "likely_cause":     rca_raw.get("likely_cause", ""),
            "recommendations":  rca_raw.get("recommendations", []),
            "confidence_score": float(rca_raw.get("confidence_score", 0.5)),
            "generated_at":     now_ms,
        },
    }

    upsert_incident(enriched)


def wait_for_kafka(bootstrap: str, retries: int = 10) -> KafkaConsumer:
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=bootstrap,
                group_id=KAFKA_GROUP,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )
            log.info("Kafka consumer connected")
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not available yet (%d/%d), retrying in 5s ...", i + 1, retries)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def main():
    log.info("AI engine starting — model=%s", OLLAMA_MODEL)
    consumer = wait_for_kafka(KAFKA_BOOTSTRAP)

    log.info("Listening for incidents on topic=%s", KAFKA_TOPIC)

    while True:
        try:
            for msg in consumer:
                try:
                    process_incident(msg.value)
                except Exception as e:
                    log.error("Failed to process incident: %s  raw=%s", e, msg.value)
        except Exception as e:
            log.error("Consumer loop error: %s — reconnecting in 5s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
