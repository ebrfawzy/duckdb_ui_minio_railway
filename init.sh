#!/bin/bash
set -euo pipefail

echo "Starting DuckDB with MinIO..."

# Check required environment variables
: "${MINIO_PUBLIC_HOST:?MINIO_PUBLIC_HOST must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"

# Set defaults for optional variables
: "${MINIO_BUCKET:=garment}"
: "${MINIO_USE_SSL:=true}"
: "${PORT:=4213}"  # Use Railway's PORT environment variable
: "${MEMORY_LIMIT:=256MB}"

echo "Configuration:"
echo "- MINIO_PUBLIC_HOST: ${MINIO_PUBLIC_HOST}"
echo "- MINIO_BUCKET: ${MINIO_BUCKET}"
echo "- MINIO_USE_SSL: ${MINIO_USE_SSL}"
echo "- PORT: ${PORT}"
echo "- MEMORY_LIMIT: ${MEMORY_LIMIT}"

exec python /app/server.py
