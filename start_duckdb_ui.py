#!/usr/bin/env python3
import os
import sys
import duckdb
import signal
import time

# Ensure unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")
DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"


def setup_and_start_ui():
    """Setup DuckDB and start UI - keeps connection alive."""
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config; exiting.", file=sys.stderr, flush=True)
        sys.exit(1)

    print(f"[setup] Connecting to database: {DB_PATH}", flush=True)
    conn = duckdb.connect(DB_PATH)

    # Configure DuckDB settings
    print("[setup] Configuring DuckDB settings...", flush=True)
    conn.execute("SET home_directory='/home/nobody';")
    conn.execute("SET enable_logging=true;")
    conn.execute("SET logging_level='debug';")
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    conn.execute("SET threads=1;")
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='512MB';")
    conn.execute("SET streaming_buffer_size='256KB';")
    conn.execute("SET enable_external_file_cache=false;")
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("SET profiling_output='';")
    conn.execute("SET ui_polling_interval=0;")

    # Install and load extensions
    print("[setup] Installing extensions...", flush=True)
    for ext in ("httpfs", "aws", "ui"):
        try:
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
            print(f"[setup] ✓ {ext} installed and loaded", flush=True)
        except Exception as e:
            print(
                f"[setup] ✗ Failed to install/load {ext}: {e}",
                file=sys.stderr,
                flush=True,
            )

    # Create PERSISTENT MinIO secret
    print("[setup] Creating persistent MinIO secret...", flush=True)
    try:
        conn.execute(
            f"""
            CREATE OR REPLACE PERSISTENT SECRET garment_minio (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{MINIO_ROOT_USER}',
                SECRET '{MINIO_ROOT_PASSWORD}',
                ENDPOINT '{MINIO_PUBLIC_HOST}',
                REGION 'us-east-1',
                URL_STYLE 'path',
                USE_SSL { 'true' if MINIO_USE_SSL else 'false' }
            );
            """
        )
        print("[setup] ✓ Persistent secret created", flush=True)
    except Exception as e:
        print(f"[setup] ✗ Failed to create secret: {e}", file=sys.stderr, flush=True)

    # Load tables from MinIO
    print(
        f"[setup] Scanning S3 bucket: s3://{MINIO_BUCKET}/db_zstd_test/*.parquet", flush=True
    )
    try:
        files = conn.execute(
            f"""
            SELECT regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name, file AS s3_path
            FROM glob('s3://{MINIO_BUCKET}/db_zstd_test/*.parquet')
            """
        ).fetchall()
        print(f"[setup] Found {len(files)} parquet files", flush=True)
    except Exception as e:
        print(f"[setup] ✗ Failed to list files: {e}", file=sys.stderr, flush=True)
        files = []

    # Create views for each table
    for table_name, s3_path in files:
        try:
            print(f"[setup] Creating view: {table_name}", flush=True)
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM parquet_scan('{s3_path}');"
            )
        except Exception as e:
            print(
                f"[setup] ✗ Failed to create view {table_name}: {e}",
                file=sys.stderr,
                flush=True,
            )

    # Summary
    tables = conn.execute("SHOW TABLES;").fetchall()
    print(f"[setup] ✓ Setup complete! Loaded {len(tables)} tables", flush=True)

    # Start DuckDB UI
    print("[ui] Starting DuckDB UI...", flush=True)
    try:
        conn.execute("CALL start_ui();")
        print("[ui] ✓ DuckDB UI started successfully", flush=True)
    except Exception as e:
        print(f"[ui] ✗ Failed to start UI: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    # Keep the connection alive and handle shutdown gracefully
    def signal_handler(signum, frame):
        print("\n[ui] Shutting down...", flush=True)
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("[ui] DuckDB UI is running. Keeping connection alive...", flush=True)

    # Keep the process alive
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n[ui] Received interrupt, shutting down...", flush=True)
        conn.close()


if __name__ == "__main__":
    setup_and_start_ui()
