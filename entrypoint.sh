#!/usr/bin/env bash
set -euo pipefail

export PORT=${PORT:-8080}
export UI_PORT=4213

# Generate nginx config
envsubst '$PORT $UI_PORT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

# Create DuckDB config to disable auth
mkdir -p /root/.duckdb
cat > /root/.duckdb/config.json << EOF
{
  "ui_require_auth": false
}
EOF

# Start DuckDB with UI in background
echo "[entrypoint] Starting DuckDB with UI..."
python3 -u /app/start_duckdb_ui.py &
DUCKDB_PID=$!

# Wait for UI to actually start (check if port is listening)
echo "[entrypoint] Waiting for DuckDB UI to be ready..."
MAX_WAIT=120
WAIT_COUNT=0

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
  # Check if Python process is still running
  if ! kill -0 $DUCKDB_PID 2>/dev/null; then
    echo "[entrypoint] ERROR: DuckDB process died!"
    exit 1
  fi
  
  # Check if port is listening (works for both IPv4 and IPv6)
  if python3 -c "import socket; s=socket.socket(socket.AF_INET6); s.settimeout(1); s.connect(('::1', $UI_PORT)); s.close()" 2>/dev/null; then
    echo "[entrypoint] ✓ DuckDB UI is ready on port $UI_PORT"
    break
  fi
  
  if [ $((WAIT_COUNT % 5)) -eq 0 ]; then
    echo "[entrypoint] Still waiting... ($WAIT_COUNT/$MAX_WAIT seconds)"
  fi
  
  WAIT_COUNT=$((WAIT_COUNT + 1))
  sleep 1
done

if [ $WAIT_COUNT -eq $MAX_WAIT ]; then
  echo "[entrypoint] ERROR: DuckDB UI failed to start within $MAX_WAIT seconds"
  echo "[entrypoint] Showing DuckDB logs:"
  cat /tmp/duckdb.log 2>/dev/null || echo "No logs available"
  exit 1
fi

# Start nginx
echo "[entrypoint] Starting nginx proxy on port ${PORT}..."
exec nginx -g 'daemon off;'