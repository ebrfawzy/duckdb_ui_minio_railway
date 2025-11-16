FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends nginx gettext-base \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir duckdb

WORKDIR /app

COPY nginx.conf.template /etc/nginx/nginx.conf.template
COPY start_duckdb_ui.py /app/start_duckdb_ui.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080
CMD ["/entrypoint.sh"]