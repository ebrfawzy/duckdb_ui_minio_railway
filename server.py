#!/usr/bin/env python3
"""
DuckDB + UI startup + pre-warm + aiohttp reverse proxy.

Sequence:
  1. Start DuckDB, set conservative resource limits, install extensions.
  2. Set ui_local_port and CALL start_ui_server() (UI binds to localhost:UI_PORT).
  3. Pre-warm the UI by doing local HTTP GET(s) to http://127.0.0.1:UI_PORT/ so the UI
     finishes fetching remote assets before external clients connect.
  4. Start aiohttp proxy on 0.0.0.0:UI_PORT which streams backend responses to clients.
"""
import os
import sys
import time
import asyncio
import duckdb
import traceback
import urllib.request
from urllib.error import URLError, HTTPError
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
    conn.execute("SET threads=1;")  # conservative for tiny containers
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


def prewarm_local_ui(
    url: str, timeout_per_try: float = 5.0, max_total: float = 25.0
) -> bool:
    """
    Try to GET `url` locally in a loop until we receive any bytes or max_total seconds elapse.
    Returns True if any successful response (HTTP or partial), False otherwise.
    This allows the UI to fetch remote assets and become responsive before external clients connect.
    """
    deadline = time.time() + max_total
    last_err = None
    headers = {"User-Agent": "duckdb-ui-prewarm/1.0"}
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=headers), timeout=timeout_per_try
            ) as resp:
                # success if we got any response (200, 302, etc.)
                # read a small chunk to ensure the UI started sending bytes
                chunk = resp.read(1)
                print(
                    f"[prewarm] got response status={resp.status}, first-byte-exists={bool(chunk)}"
                )
                return True
        except (HTTPError, URLError, TimeoutError, ConnectionResetError) as e:
            last_err = e
            # wait briefly and retry
            time.sleep(0.5)
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    print(
        f"[prewarm] failed to prewarm local UI within {max_total}s. last error: {last_err}"
    )
    return False


async def proxy_handler(request):
    """
    Proxy incoming aiohttp request to local DuckDB UI (127.0.0.1:UI_PORT),
    streaming the response back to the client as chunks arrive.
    """
    target_url = f"http://127.0.0.1:{UI_PORT}{request.rel_url}"
    timeout = ClientTimeout(total=None, sock_connect=10, sock_read=60)
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
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
    # 1) Start and setup DuckDB + UI (blocking)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, duckdb_start_and_setup)

    # 2) Pre-warm the local UI so it fetches remote assets before we accept external requests.
    local_root = f"http://127.0.0.1:{UI_PORT}/"
    print(
        f"[prewarm] attempting to prewarm local UI at {local_root} (this may take a few seconds)"
    )
    ok = prewarm_local_ui(local_root, timeout_per_try=5.0, max_total=25.0)
    if not ok:
        # we still continue — the proxy will start, but cold clients might see delays
        print(
            "[prewarm] warning: prewarm failed; proxy will start anyway (clients may see slow first loads)"
        )

    # 3) Start aiohttp proxy on 0.0.0.0:UI_PORT
    app = web.Application(client_max_size=0)
    app.add_routes([web.get("/health", health_handler)])
    app.router.add_route("*", "/{tail:.*}", proxy_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", UI_PORT, backlog=20)
    await site.start()

    print(
        f"[proxy] aiohttp proxy listening on 0.0.0.0:{UI_PORT}, proxying to http://127.0.0.1:{UI_PORT}"
    )

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
