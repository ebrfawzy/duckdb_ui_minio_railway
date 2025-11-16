FROM python:3.11-slim-bookworm
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    nginx gettext-base ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp/client_temp /tmp/proxy_temp /tmp/fastcgi_temp /tmp/uwsgi_temp /tmp/scgi_temp

# Install DuckDB Python package only
RUN pip install --no-cache-dir duckdb

WORKDIR /app
RUN mkdir -p /app/data /home/nobody && chmod 777 /app/data /home/nobody

COPY nginx.conf.template /etc/nginx/nginx.conf.template
COPY start_duckdb_ui.py /app/start_duckdb_ui.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080
CMD ["/entrypoint.sh"]