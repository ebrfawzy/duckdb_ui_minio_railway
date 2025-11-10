#!/usr/bin/env python3
"""
Starts DuckDB, registers S3 parquet views, starts the DuckDB UI on localhost:<PORT>,
and runs an asyncio-based TCP proxy to expose the UI on 0.0.0.0:<PORT> without
creating OS threads per connection (suitable for constrained containers).
"""
import os
import asyncio
import duckdb
import signal
import sys
import traceback
import contextlib

MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
UI_PORT = int(os.environ.get("PORT", "8080"))
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")

DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"
os.makedirs("/app/data", exist_ok=True)


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while True:
            data = await reader.read(64 * 1024)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
        pass
    except Exception:
        traceback.print_exc()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def handle_client(
    local_reader: asyncio.StreamReader,
    local_writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
):
    # Connect to target (internal DuckDB UI on localhost)
    try:
        remote_reader, remote_writer = await asyncio.open_connection(
            target_host, target_port
        )
    except Exception:
        try:
            local_writer.close()
            await local_writer.wait_closed()
        except Exception:
            pass
        return

    # Start bi-directional forwarding tasks
    to_remote = asyncio.create_task(pipe(local_reader, remote_writer))
    to_local = asyncio.create_task(pipe(remote_reader, local_writer))

    # Wait until one direction completes, then cancel the other
    done, pending = await asyncio.wait(
        [to_remote, to_local], return_when=asyncio.FIRST_COMPLETED
    )
    for p in pending:
        p.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await p

    # ensure closures
    try:
        remote_writer.close()
        await remote_writer.wait_closed()
    except Exception:
        pass
    try:
        local_writer.close()
        await local_writer.wait_closed()
    except Exception:
        pass


async def start_proxy(listen_port: int, target_host: str, target_port: int):
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, target_host, target_port),
        host="0.0.0.0",
        port=listen_port,
        backlog=20,
    )
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"[proxy] Listening on {addrs}, proxying to {target_host}:{target_port}")
    return server


def duckdb_start_and_setup():
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config; exiting.")
        sys.exit(1)

    conn = duckdb.connect(DB_PATH)
    # Set home for UI state
    conn.execute("SET home_directory='/home/nobody';")

    # Conservative resource limits
    conn.execute("SET enable_logging=true;")
    conn.execute("SET logging_level='debug';")
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    conn.execute("SET threads=1;")  # hard cap to 1 to be safe in constrained containers
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='512MB';")
    conn.execute("SET streaming_buffer_size='256KB';")
    conn.execute("SET enable_external_file_cache=false;")
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("SET profiling_output='';")
    conn.execute("SET ui_polling_interval=0;")

    # Load/install extensions
    for ext in ("httpfs", "aws", "ui"):
        try:
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
        except Exception as e:
            print(f"[ext] failed to install/load {ext}: {e}")

    # Create secret for MinIO
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
        print("[secret] failed:", e)

    # Register parquet views
    try:
        files = conn.execute(
            f"""
            SELECT regexp_replace(file, '.*/(.*?)\\.parquet', '\\1') AS table_name, file AS s3_path
            FROM glob('s3://{MINIO_BUCKET}/db_zstd/*.parquet')
        """
        ).fetchall()
    except Exception as e:
        print("[glob] failed to list files:", e)
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

    # Set UI port and start UI server (binds to localhost:UI_PORT)
    try:
        conn.execute(f"SET ui_local_port = {UI_PORT};")
    except Exception as e:
        print("[ui] failed to set ui_local_port:", e)
    try:
        conn.execute("CALL start_ui_server();")
        print(f"[ui] DuckDB UI started on localhost:{UI_PORT}")
    except Exception as e:
        print("[ui] start_ui_server() failed:", e)
        traceback.print_exc()
        # Let process exit so Railway can show logs / restart if necessary
        sys.exit(1)


async def main():
    # start duckdb and UI first (blocking setup)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, duckdb_start_and_setup)

    # start asyncio proxy exposing the UI on 0.0.0.0:UI_PORT -> 127.0.0.1:UI_PORT
    proxy_server = await start_proxy(UI_PORT, "127.0.0.1", UI_PORT)

    # graceful shutdown handling
    stop_event = asyncio.Event()

    def _term_handler():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _term_handler)
        except NotImplementedError:
            # add_signal_handler may be unsupported on Windows or some environments; ignore
            pass

    print(f"[ready] UI should be reachable via Railway on port {UI_PORT}")
    await stop_event.wait()
    print("[shutdown] shutting down proxy...")
    proxy_server.close()
    await proxy_server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
