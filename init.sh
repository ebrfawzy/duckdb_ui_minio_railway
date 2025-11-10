#!/bin/bash
set -euo pipefail

echo "===> init.sh starting"

# Required env vars check
: "${MINIO_PUBLIC_HOST:?MINIO_PUBLIC_HOST must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"

# Defaults (Railway overrides PORT)
: "${MINIO_BUCKET:=garment}"
: "${MINIO_USE_SSL:=true}"
: "${PORT:=8080}"
: "${MEMORY_LIMIT:=1GB}"

echo "Configuration:"
echo "- MINIO_PUBLIC_HOST: ${MINIO_PUBLIC_HOST}"
echo "- MINIO_BUCKET: ${MINIO_BUCKET}"
echo "- MINIO_USE_SSL: ${MINIO_USE_SSL}"
echo "- PORT (external): ${PORT}"
echo "- MEMORY_LIMIT: ${MEMORY_LIMIT}"

# Exec Python server (does everything: start DuckDB UI, create views, and run
# an internal Python TCP forwarder to make the UI reachable on 0.0.0.0:$PORT)
exec python /app/server.py
