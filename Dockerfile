FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install required packages including socat and netcat
RUN apt-get update && \
    apt-get install -y socat netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir duckdb

WORKDIR /app

RUN mkdir -p /home/nobody /app && \
    chown -R nobody:nogroup /home/nobody /app && \
    chmod 755 /home/nobody

ENV HOME=/home/nobody

COPY init.sh server.py ./
RUN chmod +x /app/init.sh

USER nobody

ENV PORT=8080
EXPOSE ${PORT}

CMD [ "/app/init.sh" ]