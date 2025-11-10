#!/usr/bin/env bash
set -euo pipefail

echo "===> init.sh starting"

# Required env vars
: "${MINIO_PUBLIC_HOST:?MINIO_PUBLIC_HOST must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"

# Defaults (Railway will override PORT)
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

# Exec the Python server which will:
#  - start DuckDB, create views
#  - set ui_local_port to $PORT and CALL start_ui_server()
#  - start an asyncio-based TCP proxy from 0.0.0.0:$PORT -> 127.0.0.1:$PORT
exec python /app/server.py
