#!/bin/bash
set -euo pipefail

echo "Starting DuckDB with MinIO..."

# Check required environment variables
: "${MINIO_PUBLIC_HOST:?MINIO_PUBLIC_HOST must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"

# Set defaults for optional variables, including Railway’s $PORT
: "${MINIO_BUCKET:=garment}"
: "${MINIO_USE_SSL:=true}"
: "${PORT:=8080}"
: "${MEMORY_LIMIT:=256MB}"

echo "Configuration:"
echo "- MINIO_PUBLIC_HOST: ${MINIO_PUBLIC_HOST}"
echo "- MINIO_BUCKET: ${MINIO_BUCKET}"
echo "- MINIO_USE_SSL: ${MINIO_USE_SSL}"
echo "- PORT (external): ${PORT}"
echo "- MEMORY_LIMIT: ${MEMORY_LIMIT}"

# Forward external port $PORT (0.0.0.0:$PORT) to localhost:$PORT inside the container
# using socat, so that DuckDB UI (listening on 127.0.0.1:$PORT) is reachable externally.
echo "Forwarding external port $PORT to DuckDB UI..."
socat TCP4-LISTEN:"$PORT",fork,reuseaddr TCP4:127.0.0.1:"$PORT" &

# Launch the Python server script
exec python /app/server.py
