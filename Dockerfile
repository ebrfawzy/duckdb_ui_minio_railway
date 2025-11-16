FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    nginx gettext-base ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp/client_temp /tmp/proxy_temp /tmp/fastcgi_temp /tmp/uwsgi_temp /tmp/scgi_temp

# Download DuckDB CLI binary
ARG DUCKDB_VERSION=v1.4.1
RUN curl -L "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" -o /tmp/duckdb.zip \
    && unzip /tmp/duckdb.zip -d /tmp \
    && mv /tmp/duckdb /usr/local/bin/duckdb \
    && chmod +x /usr/local/bin/duckdb \
    && rm /tmp/duckdb.zip

WORKDIR /app
COPY nginx.conf.template /etc/nginx/nginx.conf.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080
CMD ["/entrypoint.sh"]