"""
api-server/http_bridge.py

HTTP/JSON shim in front of the gRPC server.
The dashboard can't speak gRPC so this bridge translates.

Run: uvicorn http_bridge:app --port 8082
"""

import logging

import grpc
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import incident_pb2
import incident_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger("http-bridge")

GRPC_TARGET = "localhost:50051"

app = FastAPI(title="Incident Analyzer HTTP Bridge")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_SEV_NAMES    = {0: "UNKNOWN", 1: "LOW", 2: "MEDIUM", 3: "HIGH", 4: "CRITICAL"}
_STATUS_NAMES = {0: "UNKNOWN", 1: "OPEN", 2: "INVESTIGATING", 3: "RESOLVED"}


def grpc_channel():
    return grpc.insecure_channel(GRPC_TARGET)


def incident_to_dict(i: incident_pb2.Incident) -> dict:
    rca = i.rca
    return {
        "id":              i.id,
        "title":           i.title,
        "description":     i.description,
        "severity":        _SEV_NAMES.get(i.severity, "UNKNOWN"),
        "status":          _STATUS_NAMES.get(i.status, "UNKNOWN"),
        "service":         i.service,
        "startedAt":       i.started_at,
        "updatedAt":       i.updated_at,
        "relatedEventIds": list(i.related_event_ids),
        "rca": {
            "summary":          rca.summary,
            "likely_cause":     rca.likely_cause,
            "recommendations":  list(rca.recommendations),
            "confidence_score": rca.confidence_score,
            "generated_at":     rca.generated_at,
        } if rca.summary else None,
    }


@app.get("/incidents")
def list_incidents(
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    req = incident_pb2.ListIncidentsRequest(page=page, page_size=page_size)
    try:
        with grpc_channel() as ch:
            stub = incident_pb2_grpc.IncidentServiceStub(ch)
            res  = stub.ListIncidents(req, timeout=5)
            return {"incidents": [incident_to_dict(i) for i in res.incidents], "total": res.total}
    except grpc.RpcError as e:
        raise HTTPException(status_code=503, detail=e.details())


@app.get("/incidents/{incident_id}")
def get_incident(incident_id: str):
    try:
        with grpc_channel() as ch:
            stub = incident_pb2_grpc.IncidentServiceStub(ch)
            res  = stub.GetIncident(incident_pb2.GetIncidentRequest(id=incident_id), timeout=5)
            return incident_to_dict(res.incident)
    except grpc.RpcError as e:
        status = 404 if e.code() == grpc.StatusCode.NOT_FOUND else 503
        raise HTTPException(status_code=status, detail=e.details())


@app.post("/incidents/{incident_id}/analyse")
def trigger_analysis(incident_id: str):
    try:
        with grpc_channel() as ch:
            stub = incident_pb2_grpc.AnalysisServiceStub(ch)
            rca  = stub.TriggerAnalysis(incident_pb2.GetIncidentRequest(id=incident_id), timeout=10)
            return {
                "queued":           True,
                "summary":          rca.summary,
                "likely_cause":     rca.likely_cause,
                "recommendations":  list(rca.recommendations),
                "confidence_score": rca.confidence_score,
            }
    except grpc.RpcError as e:
        status = 404 if e.code() == grpc.StatusCode.NOT_FOUND else 503
        raise HTTPException(status_code=status, detail=e.details())