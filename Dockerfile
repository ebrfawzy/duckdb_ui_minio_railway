FROM python:3.11-slim

# avoid prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    HOME=/home/nobody

# Install minimal system deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
RUN pip install --no-cache-dir duckdb

WORKDIR /app

# Prepare directories and permissions for DuckDB UI state
RUN mkdir -p /app /app/data /home/nobody/.duckdb/extension_data/ui \
    && chown -R nobody:nogroup /app /home/nobody \
    && chmod 755 /home/nobody

# copy application files
COPY init.sh server.py ./

RUN chmod +x /app/init.sh

# run as less-privileged user
USER nobody

# Railway will set PORT at runtime; default to 8080
ENV PORT=8080
EXPOSE 8080

CMD [ "/app/init.sh" ]
