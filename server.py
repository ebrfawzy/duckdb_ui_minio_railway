import os
import duckdb

# Configuration from environment variables
MINIO_ENDPOINT = os.environ.get("MINIO_PRIVATE_ENDPOINT")
if MINIO_ENDPOINT and not MINIO_ENDPOINT.startswith(("http://", "https://")):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"
    MINIO_USE_SSL = False
else:
    MINIO_USE_SSL = True

MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
UI_PORT = int(os.environ.get("PORT", "8080"))  # Use Railway's PORT env var
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "256MB")
THREADS = min(int(os.cpu_count()), 4)  # Limit max threads

DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"
os.makedirs("/app/data", exist_ok=True)


def main():
    if not (MINIO_ENDPOINT and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config.")
        return

    # Initialize DuckDB with optimized settings
    conn = duckdb.connect(DB_PATH)

    # Set home directory for extensions
    conn.execute("SET home_directory='/app/data';")

    # Memory and performance settings
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    conn.execute(f"SET threads={THREADS};")
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='512MB';")  # Reduced temp dir size
    conn.execute("SET streaming_buffer_size='512KB';")  # Reduced buffer size

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

    # Configure MinIO access
    conn.execute("""SET s3_url_style='path';""")  # Force path-style URLs
    conn.execute(f"""SET s3_endpoint='{MINIO_ENDPOINT}';""")
    conn.execute(f"""SET s3_access_key_id='{MINIO_ROOT_USER}';""")
    conn.execute(f"""SET s3_secret_access_key='{MINIO_ROOT_PASSWORD}';""")
    conn.execute(f"""SET s3_use_ssl={str(MINIO_USE_SSL).lower()};""")
    conn.execute("""SET s3_region='us-east-1';""")

    # Test connection with a simple list operation
    print("Testing MinIO connection...")
    try:
        files = conn.execute(
            f"""
            SELECT 
                regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name,
                file AS s3_path 
            FROM glob('s3://{MINIO_BUCKET}/db_zstd/*.parquet')
            """
        ).fetchall()
        print("Successfully connected to MinIO")
    except Exception as e:
        print(f"Error connecting to MinIO: {str(e)}")
        print(f"Endpoint: {MINIO_ENDPOINT}")
        print(f"SSL: {MINIO_USE_SSL}")
        raise

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


if __name__ == "__main__":
    main()
