"""
api-server/main.py

gRPC server exposing:
  - IncidentService.ListIncidents
  - AnalysisService.TriggerAnalysis
"""

import json
import logging
import time
from concurrent import futures

import grpc
from kafka import KafkaProducer
from pymongo import MongoClient

import incident_pb2
import incident_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger("api-server")

GRPC_PORT  = 50051
MONGO_URI  = "mongodb://admin:incident123@localhost:27017"
MONGO_DB   = "incidents"
KAFKA_BOOTSTRAP = "localhost:9092"

_SEVERITY_MAP = {
    "LOW":      incident_pb2.SEVERITY_LOW,
    "MEDIUM":   incident_pb2.SEVERITY_MEDIUM,
    "HIGH":     incident_pb2.SEVERITY_HIGH,
    "CRITICAL": incident_pb2.SEVERITY_CRITICAL,
}

_STATUS_MAP = {
    "OPEN":          incident_pb2.STATUS_OPEN,
    "INVESTIGATING": incident_pb2.STATUS_INVESTIGATING,
    "RESOLVED":      incident_pb2.STATUS_RESOLVED,
}


def get_col(name: str):
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return client[MONGO_DB][name]


def doc_to_proto(doc: dict) -> incident_pb2.Incident:
    rca_doc = doc.get("rca") or {}
    rca = incident_pb2.RootCauseAnalysis(
        summary          = rca_doc.get("summary", ""),
        likely_cause     = rca_doc.get("likely_cause", ""),
        recommendations  = rca_doc.get("recommendations", []),
        confidence_score = float(rca_doc.get("confidence_score", 0.0)),
        generated_at     = int(rca_doc.get("generated_at", 0)),
    )
    return incident_pb2.Incident(
        id                = doc.get("id", ""),
        title             = doc.get("title", ""),
        description       = doc.get("description", ""),
        severity          = _SEVERITY_MAP.get(doc.get("severity", ""), incident_pb2.SEVERITY_UNKNOWN),
        status            = _STATUS_MAP.get(doc.get("status", ""), incident_pb2.STATUS_UNKNOWN),
        service           = doc.get("service", ""),
        started_at        = int(doc.get("startedAt", 0)),
        updated_at        = int(doc.get("updatedAt", 0)),
        related_event_ids = doc.get("relatedEventIds", []),
        rca               = rca,
    )


class IncidentServicer(incident_pb2_grpc.IncidentServiceServicer):

    def ListIncidents(self, request, context):
        page      = max(request.page, 1)
        page_size = request.page_size if request.page_size > 0 else 20
        skip      = (page - 1) * page_size

        col   = get_col("incidents")
        total = col.count_documents({})
        docs  = list(col.find({}, {"_id": 0}).sort("startedAt", -1).skip(skip).limit(page_size))

        return incident_pb2.ListIncidentsResponse(
            incidents=[doc_to_proto(d) for d in docs],
            total=total,
        )


class AnalysisServicer(incident_pb2_grpc.AnalysisServiceServicer):

    def TriggerAnalysis(self, request, context):
        col = get_col("incidents")
        doc = col.find_one({"id": request.id}, {"_id": 0})
        if not doc:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Incident {request.id!r} not found")

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send("incidents", value=doc).get(timeout=5)
        producer.close()

        log.info("Re-queued incident %s for RCA re-generation", request.id)
        rca = doc.get("rca") or {}
        return incident_pb2.RootCauseAnalysis(
            summary          = rca.get("summary", "Re-analysis queued..."),
            likely_cause     = rca.get("likely_cause", ""),
            recommendations  = rca.get("recommendations", []),
            confidence_score = float(rca.get("confidence_score", 0.0)),
            generated_at     = int(rca.get("generated_at", 0)),
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    incident_pb2_grpc.add_IncidentServiceServicer_to_server(IncidentServicer(), server)
    incident_pb2_grpc.add_AnalysisServiceServicer_to_server(AnalysisServicer(), server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    log.info("gRPC server listening on port %d", GRPC_PORT)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.stop(grace=5)


if __name__ == "__main__":
    serve()