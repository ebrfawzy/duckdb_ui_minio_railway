#!/usr/bin/env bash
set -euo pipefail

echo "===> init.sh starting"

# Required env vars
: "${MINIO_PUBLIC_HOST:?MINIO_PUBLIC_HOST must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"

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

# Exec the Python server which:
#  - starts DuckDB and its UI on localhost:$PORT
#  - pre-warms the UI by requesting the local UI root so remote asset fetch completes
#  - runs an aiohttp-based HTTP reverse-proxy on 0.0.0.0:$PORT that streams responses
exec python /app/server.py
