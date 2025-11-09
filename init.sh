#!/bin/bash
set -euo pipefail

echo "Starting DuckDB with MinIO..."

# Check required environment variables
: "${MINIO_PRIVATE_ENDPOINT:?MINIO_PRIVATE_ENDPOINT must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"

# Set defaults for optional variables
: "${MINIO_BUCKET:=garment}"
: "${PORT:=8080}"  # Use Railway's PORT environment variable
: "${MEMORY_LIMIT:=256MB}"

echo "Configuration:"
echo "- MINIO_PRIVATE_ENDPOINT: ${MINIO_PRIVATE_ENDPOINT}"
echo "- MINIO_BUCKET: ${MINIO_BUCKET}"
echo "- PORT: ${PORT}"
echo "- MEMORY_LIMIT: ${MEMORY_LIMIT}"

exec python /app/server.py
