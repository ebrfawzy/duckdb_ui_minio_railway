#!/usr/bin/env bash
set -euo pipefail

export PORT=${PORT:-8080}
export UI_PORT=4213

# Generate nginx config
envsubst '$PORT $UI_PORT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

# Run Python setup
echo "[entrypoint] Running DuckDB setup..."
python3 /app/setup_duckdb.py

# Determine database path from MINIO_BUCKET
MINIO_BUCKET=${MINIO_BUCKET:-garment}
DB_PATH="/app/data/${MINIO_BUCKET}.duckdb"

# Start DuckDB UI with the configured database
echo "[entrypoint] Starting DuckDB UI on 0.0.0.0:${UI_PORT}..."
(
  sleep 2
  echo "LOAD ui; CALL start_ui();"
  tail -f /dev/null
) | duckdb "$DB_PATH" > /dev/null 2>&1 &

# Wait for UI to start
sleep 5

# Start nginx
echo "[entrypoint] Starting nginx proxy on port ${PORT}..."
exec nginx -g 'daemon off;'