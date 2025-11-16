#!/bin/bash
set -euo pipefail

export PORT=${PORT:-8080}
export UI_PORT=4213

envsubst '$PORT $UI_PORT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf
python3 -u /app/start_duckdb_ui.py &
echo "=========================================="
echo "DuckDB UI starting at http://localhost:$UI_PORT"
echo "nginx started at http://localhost:$PORT"
echo "=========================================="
exec nginx -g 'daemon off;'