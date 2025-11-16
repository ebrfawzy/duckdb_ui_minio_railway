#!/usr/bin/env bash
set -euo pipefail

export PORT=${PORT:-8080}
export UI_PORT=4213

# Generate nginx config with proper substitution
envsubst '$PORT $UI_PORT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

# Start DuckDB UI
(
  sleep 2
  echo "INSTALL ui; LOAD ui; CALL start_ui();"
  tail -f /dev/null
) | duckdb /app/data.db > /dev/null 2>&1 &

# Wait for UI to start
sleep 5

# Start nginx
exec nginx -g 'daemon off;'