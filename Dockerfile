FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    HOME=/home/nobody \
    PORT=8080

# Install a minimal set of system packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
RUN pip install --no-cache-dir duckdb

WORKDIR /app

# Prepare directories and permissions for DuckDB UI state
RUN mkdir -p /app /app/data /home/nobody/.duckdb/extension_data/ui \
    && chown -R nobody:nogroup /app /home/nobody \
    && chmod 755 /home/nobody

# Copy application files
COPY init.sh server.py ./
RUN chmod +x /app/init.sh

# Run as unprivileged user
USER nobody

EXPOSE 8080

CMD [ "/app/init.sh" ]
