#!/usr/bin/env bash
# Run this once to generate Python gRPC stubs from the proto definition.
# Output lands in api-server/ so the server can import them directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR/../proto"
OUT_DIR="$SCRIPT_DIR/../api-server"

echo "Generating stubs from $PROTO_DIR/incident.proto → $OUT_DIR"

pip install grpcio-tools --quiet

python -m grpc_tools.protoc \
  -I "$PROTO_DIR" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_DIR/incident.proto"

echo "Done. Generated files:"
ls "$OUT_DIR"/incident_pb2*.py
