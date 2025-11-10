#!/usr/bin/env python3
import os
import time
import socket
import threading
import duckdb
import traceback

# -------- Configuration --------
MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
UI_PORT = int(os.environ.get("PORT", "8080"))  # Railway-provided PORT (external)
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")

DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"
os.makedirs("/app/data", exist_ok=True)


# -------- Minimal, robust TCP forwarder (pure Python) --------
# Accepts connections on 0.0.0.0:listen_port and proxies them to target_host:target_port.
# Uses threads (daemon) for simplicity and to avoid forking new processes.
BUF_SIZE = 64 * 1024


def forward(src, dst):
    try:
        while True:
            data = src.recv(BUF_SIZE)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        # connection errors are expected when a client closes quickly; ignore
        pass
    finally:
        try:
            src.shutdown(socket.SHUT_RD)
        except Exception:
            pass


def handle_client(client_sock, target_host, target_port):
    try:
        target = socket.create_connection((target_host, target_port))
    except Exception as e:
        # if we cannot connect to target, close client and return
        try:
            client_sock.close()
        except Exception:
            pass
        return

    # spawn two threads to pipe data in both directions
    t1 = threading.Thread(target=forward, args=(client_sock, target), daemon=True)
    t2 = threading.Thread(target=forward, args=(target, client_sock), daemon=True)
    t1.start()
    t2.start()

    # wait for both directions to finish
    t1.join()
    t2.join()

    try:
        client_sock.close()
    except Exception:
        pass
    try:
        target.close()
    except Exception:
        pass


def start_tcp_forwarder(listen_port: int, target_host: str, target_port: int):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # reuseaddr to reduce TIME_WAIT issues
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("0.0.0.0", listen_port))
    listener.listen(100)  # reasonable backlog
    print(f"[proxy] forwarding 0.0.0.0:{listen_port} -> {target_host}:{target_port}")

    # Run accept loop in daemon thread to not block main thread
    def accept_loop():
        try:
            while True:
                client_sock, _ = listener.accept()
                th = threading.Thread(
                    target=handle_client,
                    args=(client_sock, target_host, target_port),
                    daemon=True,
                )
                th.start()
        except Exception as e:
            print("[proxy] accept loop ended:", e)
            traceback.print_exc()
        finally:
            try:
                listener.close()
            except Exception:
                pass

    t = threading.Thread(target=accept_loop, daemon=True)
    t.start()
    return listener


# -------- DuckDB startup and view wiring --------
def main():
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config; exiting.")
        return

    # connect to DuckDB
    conn = duckdb.connect(DB_PATH)

    # set home directory for UI state (non-root user HOME)
    conn.execute("SET home_directory='/home/nobody';")

    # conservative thread limits and memory usage to fit constrained containers
    conn.execute("SET enable_logging=true;")
    conn.execute("SET logging_level='debug';")
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    # hard cap threads to 2 to avoid hitting small container cgroup thread limits
    conn.execute("SET threads=2;")
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='512MB';")
    conn.execute("SET streaming_buffer_size='512KB';")

    # disable large caches
    conn.execute("SET enable_external_file_cache=false;")
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("SET profiling_output='';")
    conn.execute("SET ui_polling_interval=0;")

    # install/load required extensions
    for ext in ("httpfs", "aws", "ui"):
        try:
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
        except Exception as e:
            print(f"[ext] failed to install/load {ext}: {e}")

    # create secret for MinIO (S3)
    try:
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
                USE_SSL { 'true' if MINIO_USE_SSL else 'false' }
            );
        """
        )
    except Exception as e:
        print("[secret] failed to create secret:", e)

    # register parquet views
    try:
        files = conn.execute(
            f"""
            SELECT 
                regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name,
                file AS s3_path 
            FROM glob('s3://{MINIO_BUCKET}/db_zstd/*.parquet')
            """
        ).fetchall()
    except Exception as e:
        print("[glob] error while listing parquet files:", e)
        files = []

    for table_name, s3_path in files:
        try:
            print(f"[load] Loading table: {table_name} from {s3_path}")
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM parquet_scan('{s3_path}');"
            )
        except Exception as e:
            print(f"[load] failed for {table_name}: {e}")

    tables = conn.execute("SHOW TABLES;").fetchall()
    print(f"[info] Count of loaded tables: {len(tables)}")

    # set DuckDB UI port to the Railway port we have available
    try:
        conn.execute(f"SET ui_local_port = {UI_PORT};")
    except Exception as e:
        print("[ui] failed to set ui_local_port:", e)

    # start UI server (DuckDB's internal UI binds to localhost:UI_PORT)
    try:
        conn.execute("CALL start_ui_server();")
        print(f"[ui] DuckDB UI started on localhost:{UI_PORT} (internal)")
    except Exception as e:
        print("[ui] start_ui_server() failed:", e)
        traceback.print_exc()
        # If UI can't start, we still may want to keep process alive for debugging
        return

    # start in-process TCP forwarder to expose the UI on 0.0.0.0:UI_PORT
    try:
        # forward from external 0.0.0.0:UI_PORT to internal 127.0.0.1:UI_PORT
        start_tcp_forwarder(UI_PORT, "127.0.0.1", UI_PORT)
    except Exception as e:
        print("[proxy] failed to start tcp forwarder:", e)
        traceback.print_exc()

    print(
        f"[ready] UI should be reachable via Railway at port {UI_PORT} (proxying to localhost)."
    )

    # keep process alive so Railway's container stays up and UI remains available
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("[shutdown] received KeyboardInterrupt, exiting.")


if __name__ == "__main__":
    main()
