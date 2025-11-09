import os
import duckdb
import time
import gc
import psutil
import signal

# Configuration from environment variables
MINIO_PRIVATE_ENDPOINT = os.environ.get("MINIO_PRIVATE_ENDPOINT")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
UI_PORT = int(os.environ.get("PORT", "8080"))  # Use Railway's PORT env var
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "256MB")  # Reduced default memory limit
THREADS = min(int(os.cpu_count()), 4)  # Limit max threads

DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"
os.makedirs("/app/data", exist_ok=True)

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def cleanup_handler(signum, frame):
    """Handle cleanup on shutdown."""
    print("Shutting down gracefully...")
    gc.collect()  # Force garbage collection
    os._exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, cleanup_handler)
signal.signal(signal.SIGINT, cleanup_handler)


def main():
    if not (MINIO_PRIVATE_ENDPOINT and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config.")
        return

    # Initialize DuckDB with optimized settings
    conn = duckdb.connect(DB_PATH)
    
    # Memory and performance settings
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    conn.execute(f"SET threads={THREADS};")
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='512MB';")  # Reduced temp dir size
    conn.execute("SET streaming_buffer_size='512KB';")    # Reduced buffer size
    
    # Disable caching to reduce memory usage
    conn.execute("SET enable_external_file_cache=false;")
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("SET profiling_output='';")
    
    # Load required extensions
    for ext in ["httpfs", "aws"]:
        conn.execute(f"INSTALL {ext};")
        conn.execute(f"LOAD {ext};")

    # Define MinIO credentials and endpoint as a DuckDB secret (modern, reliable method)
    conn.execute(
        f"""
        CREATE OR REPLACE SECRET garment_minio (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{MINIO_ROOT_USER}',
            SECRET '{MINIO_ROOT_PASSWORD}',
            ENDPOINT '{MINIO_PRIVATE_ENDPOINT}',
            REGION 'us-east-1',
            URL_STYLE 'path',
            USE_SSL true
        );
    """
    )

    files = conn.execute(
        f"""
        SELECT 
            regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name,
            file AS s3_path 
        FROM glob('s3://{MINIO_BUCKET}/db_zstd/*.parquet')
        """
    ).fetchall()

    for table_name, s3_path in files:
        conn.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM parquet_scan('{s3_path}');"
        )

    tables = conn.execute("SHOW TABLES;").fetchall()
    print(f"Count of loaded tables: {len(tables)}")

    conn.execute(f"SET ui_polling_interval = 0;")
    conn.execute(f"SET ui_local_port={UI_PORT};")
    conn.execute("INSTALL ui;")
    conn.execute("LOAD ui;")
    conn.execute("CALL start_ui_server();")
    print(
        f"DuckDB UI at http://localhost:{UI_PORT}\nMem limit: {MEM_LIMIT} | Threads: {THREADS}"
    )

    memory_check_interval = 60  # Check memory every minute
    max_memory_mb = float(MEM_LIMIT.replace('MB', ''))
    restart_threshold_mb = max_memory_mb * 0.9  # 90% of memory limit

    print(f"DuckDB UI at http://0.0.0.0:{UI_PORT}")
    print(f"Memory limit: {MEM_LIMIT} | Threads: {THREADS}")
    print(f"Memory restart threshold: {restart_threshold_mb:.1f}MB")

    try:
        while True:
            time.sleep(memory_check_interval)
            current_memory = get_memory_usage()
            
            if current_memory > restart_threshold_mb:
                print(f"Memory usage ({current_memory:.1f}MB) exceeded threshold ({restart_threshold_mb:.1f}MB)")
                print("Initiating graceful restart...")
                cleanup_handler(None, None)
            
            # Force garbage collection periodically
            gc.collect()
    except Exception as e:
        print(f"Error: {e}")
        cleanup_handler(None, None)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        cleanup_handler(None, None)
