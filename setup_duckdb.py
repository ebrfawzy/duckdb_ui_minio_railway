#!/usr/bin/env python3
import os
import sys
import duckdb

MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")
DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"


def duckdb_start_and_setup():
    """Setup DuckDB: install/load extensions, create secret and views."""
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config; exiting.", file=sys.stderr)
        sys.exit(1)

    print(f"[setup] Connecting to database: {DB_PATH}")
    conn = duckdb.connect(DB_PATH)

    # Configure DuckDB settings
    print("[setup] Configuring DuckDB settings...")
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
    print("[setup] Installing extensions...")
    for ext in ("httpfs", "aws", "ui"):
        try:
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
            print(f"[setup] ✓ {ext} installed and loaded")
        except Exception as e:
            print(f"[setup] ✗ Failed to install/load {ext}: {e}", file=sys.stderr)

    # Create MinIO secret
    print("[setup] Creating MinIO secret...")
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
        print("[setup] ✓ Secret created")
    except Exception as e:
        print(f"[setup] ✗ Failed to create secret: {e}", file=sys.stderr)

    # Load tables from MinIO
    print(f"[setup] Scanning S3 bucket: s3://{MINIO_BUCKET}/db_zstd_test/*.parquet")
    try:
        files = conn.execute(
            f"""
            SELECT regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name, file AS s3_path
            FROM glob('s3://{MINIO_BUCKET}/db_zstd_test/*.parquet')
            """
        ).fetchall()
        print(f"[setup] Found {len(files)} parquet files")
    except Exception as e:
        print(f"[setup] ✗ Failed to list files: {e}", file=sys.stderr)
        files = []

    # Create views for each table
    for table_name, s3_path in files:
        try:
            print(f"[setup] Creating view: {table_name}")
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM parquet_scan('{s3_path}');"
            )
        except Exception as e:
            print(f"[setup] ✗ Failed to create view {table_name}: {e}", file=sys.stderr)

    # Summary
    tables = conn.execute("SHOW TABLES;").fetchall()
    print(f"[setup] ✓ Setup complete! Loaded {len(tables)} tables")

    conn.close()


if __name__ == "__main__":
    duckdb_start_and_setup()
