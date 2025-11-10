FROM python:3.11-slim

# Use env vars for Python behavior and app path
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install socat (for proxy) and DuckDB
RUN apt-get update && apt-get install -y socat && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir duckdb

WORKDIR /app

# Prepare home directory for the 'nobody' user (DuckDB UI stores state under $HOME/.duckdb)
RUN mkdir -p /home/nobody /app /home/nobody/.duckdb/extension_data/ui && \
    chown -R nobody:nogroup /home/nobody /app && \
    chmod 755 /home/nobody

ENV HOME=/home/nobody

# Copy application files into image and make init script executable
COPY init.sh server.py ./
RUN chmod +x /app/init.sh

# Switch to non-root user for running the app
USER nobody

# Default port (Railway will override $PORT at runtime)
ENV PORT=8080
EXPOSE ${PORT}

# Start the init script
CMD [ "/app/init.sh" ]
