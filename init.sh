#!/bin/bash
set -euo pipefail

echo "Starting DuckDB with MinIO..."

# Check required environment variables
: "${MINIO_PUBLIC_HOST:?MINIO_PUBLIC_HOST must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"

# Set defaults
: "${MINIO_BUCKET:=garment}"
: "${MINIO_USE_SSL:=true}"
: "${PORT:=8080}"
: "${MEMORY_LIMIT:=256MB}"

echo "Configuration:"
echo "- MINIO_PUBLIC_HOST: ${MINIO_PUBLIC_HOST}"
echo "- MINIO_BUCKET: ${MINIO_BUCKET}"
echo "- PORT: ${PORT}"
echo "- MEMORY_LIMIT: ${MEMORY_LIMIT}"

# Start DuckDB UI server in background
python /app/server.py &
PYTHON_PID=$!

# Wait for DuckDB UI to start on localhost:4213
echo "Waiting for DuckDB UI to start on localhost:4213..."
for i in {1..60}; do
    if nc -z 127.0.0.1 4213 2>/dev/null; then
        echo "DuckDB UI is ready on port 4213!"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "ERROR: DuckDB UI failed to start after 60 seconds"
        echo "Python process status:"
        ps aux | grep python || echo "Python process not found"
        exit 1
    fi
    sleep 1
done

# Start socat to proxy 0.0.0.0:$PORT -> 127.0.0.1:4213
echo "Starting socat proxy: 0.0.0.0:${PORT} -> 127.0.0.1:4213"
exec socat TCP4-LISTEN:${PORT},bind=0.0.0.0,fork,reuseaddr TCP4:127.0.0.1:4213