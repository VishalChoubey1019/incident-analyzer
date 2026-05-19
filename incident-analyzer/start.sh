#!/usr/bin/env bash
# start.sh — boots the entire stack in the right order.
# Run from the project root: ./start.sh

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

info()    { echo -e "${BLUE}[•]${NC} $*"; }
success() { echo -e "${GREEN}[✓]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!]${NC} $*"; }
fail()    { echo -e "${RED}[✗]${NC} $*"; exit 1; }

# ── Preflight checks ──────────────────────────────────────────────────────
command -v docker  >/dev/null 2>&1 || fail "Docker not found. Install Docker Desktop."
command -v python3 >/dev/null 2>&1 || fail "Python 3 not found."
command -v ollama  >/dev/null 2>&1 || fail "Ollama not found. Install from https://ollama.com"

info "Checking Llama 3 model is pulled ..."
if ! ollama list | grep -q "llama3"; then
  warn "llama3 not found — pulling now (this takes a few minutes, ~4GB) ..."
  ollama pull llama3
fi
success "Llama 3 ready"

# ── Infrastructure ────────────────────────────────────────────────────────
info "Starting Docker infrastructure (Kafka, MongoDB, Flink) ..."
docker compose -f docker/docker-compose.yml up -d

info "Waiting for Kafka to be ready ..."
until docker exec ia-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done
success "Kafka ready"

info "Creating Kafka topics ..."
for topic in logs alerts incidents; do
  docker exec ia-kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" \
    --partitions 3 --replication-factor 1 2>/dev/null
done
success "Topics ready: logs, alerts, incidents"

# ── Python virtualenvs ────────────────────────────────────────────────────
setup_venv() {
  local dir="$1"
  info "Setting up venv for $dir ..."
  cd "$ROOT/$dir"
  python3 -m venv .venv
  .venv/bin/pip install -q -r requirements.txt
  success "$dir venv ready"
  cd "$ROOT"
}

setup_venv ingest-service
setup_venv ai-engine
setup_venv api-server

# ── Generate gRPC stubs ────────────────────────────────────────────────────
info "Generating gRPC Python stubs ..."
cd "$ROOT/api-server"
.venv/bin/python -m grpc_tools.protoc \
  -I"$ROOT/proto" \
  --python_out=. \
  --grpc_python_out=. \
  "$ROOT/proto/incident.proto"
success "Stubs generated"
cd "$ROOT"

# ── Start services ────────────────────────────────────────────────────────
info "Starting ingest service on :8080 ..."
cd "$ROOT/ingest-service"
.venv/bin/uvicorn main:app --port 8080 --log-level warning &
INGEST_PID=$!
cd "$ROOT"

info "Starting AI engine ..."
cd "$ROOT/ai-engine"
.venv/bin/python main.py &
AI_PID=$!
cd "$ROOT"

info "Starting gRPC API server on :50051 ..."
cd "$ROOT/api-server"
.venv/bin/python main.py &
GRPC_PID=$!

sleep 1

info "Starting HTTP bridge on :8082 ..."
.venv/bin/uvicorn http_bridge:app --port 8082 --log-level warning &
BRIDGE_PID=$!
cd "$ROOT"

sleep 2

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
success "All services running"
echo ""
echo -e "  Dashboard     → ${BLUE}open dashboard/index.html${NC} in your browser"
echo -e "  Ingest API    → ${BLUE}http://localhost:8080/docs${NC}"
echo -e "  HTTP Bridge   → ${BLUE}http://localhost:8082/docs${NC}"
echo -e "  Kafka UI      → ${BLUE}http://localhost:9093${NC}"
echo -e "  Flink UI      → ${BLUE}http://localhost:8081${NC}"
echo ""
echo -e "  To fire test incidents:"
echo -e "    ${YELLOW}cd scripts && python3 emit_incidents.py --all${NC}"
echo ""
echo -e "  PIDs  ingest=$INGEST_PID  ai-engine=$AI_PID  grpc=$GRPC_PID  bridge=$BRIDGE_PID"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Trap Ctrl-C and kill all background services
trap "echo ''; info 'Shutting down...'; kill $INGEST_PID $AI_PID $GRPC_PID $BRIDGE_PID 2>/dev/null; docker compose -f docker/docker-compose.yml stop; success 'Stopped.'" SIGINT SIGTERM

wait
