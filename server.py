#!/usr/bin/env python3
"""
Starts DuckDB, registers S3 parquet views, starts the DuckDB UI on localhost:<PORT>,
and runs an aiohttp-based HTTP reverse proxy on 0.0.0.0:<PORT> that streams
the backend response to the client. Also provides a fast /health endpoint.
"""
import os
import sys
import asyncio
import duckdb
import traceback
from aiohttp import web, ClientSession, ClientTimeout

MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
UI_PORT = int(os.environ.get("PORT", "8080"))
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")

DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"
os.makedirs("/app/data", exist_ok=True)


def duckdb_start_and_setup():
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config; exiting.", file=sys.stderr)
        sys.exit(1)

    conn = duckdb.connect(DB_PATH)

    # home directory for UI state
    conn.execute("SET home_directory='/home/nobody';")

    # conservative resource limits
    conn.execute("SET enable_logging=true;")
    conn.execute("SET logging_level='debug';")
    conn.execute(f"SET memory_limit='{MEM_LIMIT}';")
    conn.execute("SET threads=1;")  # conservative
    conn.execute("SET temp_directory='/tmp';")
    conn.execute("SET max_temp_directory_size='512MB';")
    conn.execute("SET streaming_buffer_size='256KB';")
    conn.execute("SET enable_external_file_cache=false;")
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("SET profiling_output='';")
    conn.execute("SET ui_polling_interval=0;")

    # install/load extensions
    for ext in ("httpfs", "aws", "ui"):
        try:
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
        except Exception as e:
            print(f"[ext] failed to install/load {ext}: {e}")

    # create secret for MinIO
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

    # set UI port and start UI server (binds to localhost:UI_PORT)
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
        sys.exit(1)


async def proxy_handler(request):
    """
    Proxy incoming aiohttp request to local DuckDB UI (127.0.0.1:UI_PORT),
    streaming the response back to the client as chunks arrive.
    """
    target_url = f"http://127.0.0.1:{UI_PORT}{request.rel_url}"

    # short-ish client timeout for connection/first-byte (adjust as needed)
    timeout = ClientTimeout(total=None, sock_connect=10, sock_read=60)

    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    # preserve method and body
    data = await request.read()

    async with ClientSession(timeout=timeout) as session:
        try:
            async with session.request(
                request.method,
                target_url,
                headers=headers,
                data=data,
                allow_redirects=False,
            ) as resp:
                # Prepare response headers (filter hop-by-hop)
                excluded = {
                    "transfer-encoding",
                    "connection",
                    "keep-alive",
                    "proxy-authenticate",
                    "proxy-authorization",
                    "te",
                    "trailers",
                    "upgrade",
                }
                headers_out = [
                    (k, v) for k, v in resp.headers.items() if k.lower() not in excluded
                ]

                # Stream response to client as it arrives
                response = web.StreamResponse(
                    status=resp.status, reason=resp.reason, headers=headers_out
                )
                await response.prepare(request)

                async for chunk in resp.content.iter_chunked(64 * 1024):
                    if not chunk:
                        break
                    await response.write(chunk)
                await response.write_eof()
                return response
        except asyncio.TimeoutError:
            return web.Response(
                status=504, text="Gateway timeout while contacting local DuckDB UI."
            )
        except Exception as e:
            traceback.print_exc()
            return web.Response(status=502, text=f"Bad gateway: {e}")


async def health_handler(request):
    return web.Response(text="ok", status=200)


async def main():
    # Start DuckDB setup in executor (blocking)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, duckdb_start_and_setup)

    # set up aiohttp app
    app = web.Application(client_max_size=0)  # unlimited client upload size
    app.add_routes([web.get("/health", health_handler)])
    # catch-all route for proxying everything else
    app.router.add_route("*", "/{tail:.*}", proxy_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", UI_PORT, backlog=20)
    await site.start()

    print(
        f"[proxy] aiohttp proxy listening on 0.0.0.0:{UI_PORT}, proxying to http://127.0.0.1:{UI_PORT}"
    )
    # keep running until killed
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted, exiting.")
    except Exception:
        traceback.print_exc()
        sys.exit(1)
