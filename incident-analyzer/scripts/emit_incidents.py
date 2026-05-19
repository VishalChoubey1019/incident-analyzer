#!/usr/bin/env python3
"""
scripts/emit_incidents.py
Fires fake logs and alerts at the ingest service to simulate a production incident.

Usage:
    python3 emit_incidents.py
"""

import time
from datetime import datetime, timezone

import httpx

INGEST_BASE = "http://localhost:8080"

LOGS = [
    ("order-service", "ERROR", "FATAL: Unable to acquire connection from pool after 30s timeout"),
    ("order-service", "ERROR", "Query failed - too many connections"),
    ("order-service", "ERROR", "Retry attempt 1/3 failed for order creation"),
    ("order-service", "ERROR", "Retry attempt 2/3 failed for order creation"),
    ("order-service", "ERROR", "Retry attempt 3/3 failed — dropping request"),
    ("order-service", "ERROR", "Circuit breaker OPEN for database calls"),
]

def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def post_log(service, level, message):
    payload = {
        "service":   service,
        "level":     level,
        "message":   message,
        "host":      "unknown",
        "timestamp": now_ms(),
        "labels":    {},
    }
    r = httpx.post(f"{INGEST_BASE}/events/log", json=payload, timeout=5)
    print(f"  log [{level}] {service}: {message[:60]}  → {r.status_code}")

def post_alert(service, severity, name, message):
    payload = {
        "name":        name,
        "source":      "prometheus",
        "severity":    severity,
        "service":     service,
        "message":     message,
        "annotations": {},
    }
    r = httpx.post(f"{INGEST_BASE}/events/alert", json=payload, timeout=5)
    print(f"  alert [{severity}] {name}  → {r.status_code}")

def main():
    try:
        httpx.get(f"{INGEST_BASE}/docs", timeout=3)
        print(f"Ingest service reachable at {INGEST_BASE}\n")
    except Exception:
        print(f"Ingest service not reachable at {INGEST_BASE} — is it running?")
        return

    post_alert("order-service", "CRITICAL", "DatabaseConnectionPoolExhausted", "Connection pool exhausted")

    for service, level, message in LOGS:
        time.sleep(0.5)
        post_log(service, level, message)

    print("\nDone — check the dashboard in ~15 seconds")

if __name__ == "__main__":
    main()