import os
import time
import duckdb

# Configuration from environment variables
MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
UI_PORT = int(os.environ.get("PORT", "8080"))  # Railway-provided PORT
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")
THREADS = min(int(os.cpu_count()), 4)  # Limit threads for performance

DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"
os.makedirs("/app/data", exist_ok=True)


def main():
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config.")
        return

    # Connect to DuckDB (creates file if it doesn't exist)
    conn = duckdb.connect(DB_PATH)

    # Set DuckDB home directory (for UI state) to the 'nobody' home
    conn.execute("SET home_directory='/home/nobody';")

    # Performance and resource settings
    conn.execute("SET enable_logging=true;")
    conn.execute("SET logging_level='debug';")
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    conn.execute(f"SET threads={THREADS};")
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='512MB';")
    conn.execute("SET streaming_buffer_size='512KB';")
    conn.execute("SET enable_external_file_cache=false;")
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("SET profiling_output='';")
    conn.execute("SET ui_polling_interval=0;")  # Disable UI polling for efficiency

    # Load required extensions
    for ext in ["httpfs", "aws", "ui"]:
        conn.execute(f"INSTALL {ext};")
        conn.execute(f"LOAD {ext};")

    # Define MinIO (S3) credentials as a DuckDB secret
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
            USE_SSL true
        );
    """
    )

    # Load parquet files from MinIO as DuckDB views
    files = conn.execute(
        f"""
        SELECT 
            regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name,
            file AS s3_path 
        FROM glob('s3://{MINIO_BUCKET}/db_zstd/*.parquet')
    """
    ).fetchall()

    for table_name, s3_path in files:
        print(f"Loading table: {table_name} from {s3_path}")
        conn.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM parquet_scan('{s3_path}');"
        )

    tables = conn.execute("SHOW TABLES;").fetchall()
    print(f"Count of loaded tables: {len(tables)}")

    # Configure the DuckDB UI to use the desired port
    conn.execute(f"SET ui_local_port = {UI_PORT};")
    # Start the DuckDB UI server (binds to 127.0.0.1:UI_PORT)
    conn.execute("CALL start_ui_server();")
    print(f"DuckDB UI running on port {UI_PORT} (bound to localhost)")

    # Keep the script running so the UI remains accessible
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down UI server.")


if __name__ == "__main__":
    main()
