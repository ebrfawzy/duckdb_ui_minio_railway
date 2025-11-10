FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install required packages
RUN apt-get update && \
    apt-get install -y socat netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir duckdb

WORKDIR /app

# Create necessary directories with proper permissions
RUN mkdir -p /home/nobody/.duckdb/extension_data /app/data && \
    chown -R nobody:nogroup /home/nobody /app && \
    chmod -R 755 /home/nobody

ENV HOME=/home/nobody

COPY init.sh server.py ./
RUN chmod +x /app/init.sh

USER nobody

ENV PORT=8080
EXPOSE ${PORT}

CMD [ "/app/init.sh" ]