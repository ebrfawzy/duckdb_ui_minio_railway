FROM python:3.11-slim

# Set environment variables for better Python behavior in containers
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install required Python packages
RUN pip install --no-cache-dir duckdb

WORKDIR /app

# Create directories and set permissions
RUN mkdir -p /app/data && \
    mkdir -p /tmp && \
    chown -R nobody:nogroup /app /tmp

# Copy application files
COPY init.sh server.py ./
RUN chmod +x /app/init.sh

# Use non-root user for security
USER nobody

# Health check to help Railway monitor the container
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/ || exit 1

# Default port (Railway will override with their PORT env var)
ENV PORT=8080
EXPOSE ${PORT}

CMD [ "/app/init.sh" ]
