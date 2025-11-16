#!/usr/bin/env python3
import os
import sys
import duckdb
import time

MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_PARQUET_PATH = os.environ.get("MINIO_PARQUET_PATH", "db_zstd_test")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")
THREADS = int(os.cpu_count())
DB_PATH = f"{MINIO_BUCKET}.duckdb"


def main():
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config.")
        sys.exit(1)

    print("Starting DuckDB UI...")
    print(f"DuckDB DB path: {DB_PATH}")
    print(f"Memory limit: {MEM_LIMIT}, Threads: {THREADS}")

    conn = duckdb.connect(DB_PATH)

    # ---------------------------------------------
    # DuckDB minimal-resource configuration (Railway)
    # ---------------------------------------------
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    conn.execute(f"SET threads={THREADS};")

    # CPU / threads
    conn.execute("SET external_threads=1;")
    conn.execute("SET pin_threads='off';")

    # Temp + memory behavior
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='256MB';")
    conn.execute("SET streaming_buffer_size='256KB';")
    conn.execute("SET allocator_flush_threshold='64.0 MiB';")
    conn.execute("SET allocator_bulk_deallocation_flush_threshold='128.0 MiB';")

    # Install & load extensions
    for ext in ("httpfs", "aws", "ui"):
        conn.execute(f"INSTALL {ext}; LOAD {ext};")

    # Auto extension behavior
    conn.execute("SET autoload_known_extensions=false;")
    conn.execute("SET autoinstall_known_extensions=false;")

    # Reduce filesystem access to ONLY S3
    conn.execute("SET disabled_filesystems='http,https,gcs,azure,file'")

    # Disable caches & heavy features
    conn.execute("SET enable_external_file_cache=false;")
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET parquet_metadata_cache=false;")
    conn.execute("SET disable_parquet_prefetching=true;")
    conn.execute("SET prefetch_all_parquet_files=false;")

    # Logging & profiling
    conn.execute("SET enable_logging=false;")
    conn.execute("SET profiling_output='';")
    conn.execute("SET enable_progress_bar=false;")

    # HTTP tuning
    conn.execute("SET http_keep_alive=false;")
    conn.execute("SET http_retries=1;")
    conn.execute("SET http_timeout=10;")

    # Preserve minimal behavior
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET preserve_insertion_order=false;")

    # UI-specific
    conn.execute("SET ui_polling_interval=0;")
    # ---------------------------------------------

    conn.execute(
        f"""
        CREATE OR REPLACE SECRET garment_minio (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{MINIO_ROOT_USER}',
            SECRET '{MINIO_ROOT_PASSWORD}',
            ENDPOINT '{MINIO_PUBLIC_HOST}',
            REGION 'us-east-1',
            URL_STYLE 'path',
            USE_SSL {'true' if MINIO_USE_SSL else 'false'}
        );
    """
    )
    print("Configured DuckDB variables, extensions, and MinIO secret.")

    files = conn.execute(
        f"""
        SELECT regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name, file AS s3_path
        FROM glob('s3://{MINIO_BUCKET}/{MINIO_PARQUET_PATH}/*.parquet')
    """
    ).fetchall()

    for table_name, s3_path in files:
        print(f"Loading view {table_name} from {s3_path}")
        conn.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM parquet_scan('{s3_path}');"
        )

    conn.execute("CALL start_ui_server();")
    print("DuckDB UI started.")

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
