#!/usr/bin/env python3
"""
Robust DuckDB + UI startup + prewarm + aiohttp proxy.

Behavior:
 - Start DuckDB and UI (ui_local_port set to PORT).
 - Wait for localhost:PORT to accept TCP (configurable timeout).
 - Pre-warm (optional); if pre-warm times out, start proxy but return 503 to clients
   until backend becomes healthy.
 - Proxy streams backend responses; upstream connect timeout is short so errors
   return quickly instead of causing long Railway edge waits (499/504).
"""
import os
import sys
import time
import socket
import asyncio
import duckdb
import traceback
from aiohttp import web, ClientSession, ClientTimeout

# Configuration from environment
MINIO_PUBLIC_HOST = os.environ.get("MINIO_PUBLIC_HOST")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "garment")
MINIO_USE_SSL = os.environ.get("MINIO_USE_SSL", "true").lower() in ("1", "true", "yes")
UI_PORT = int(os.environ.get("PORT", "8080"))
MEM_LIMIT = os.environ.get("MEMORY_LIMIT", "1GB")

# Timing / timeout tunables
BACKEND_TCP_WAIT = 45.0  # seconds to wait for the UI TCP port to accept connections
PREWARM_TOTAL = 30.0  # seconds allowed for prewarm (local GET to /)
PREWARM_TRY_TIMEOUT = 3.0  # per-try timeout when prewarming local UI
UPSTREAM_CONNECT_TIMEOUT = 3.0  # aiohttp connect timeout to backend
UPSTREAM_READ_TIMEOUT = 30.0  # aiohttp sock_read timeout

DB_PATH = f"/app/data/{MINIO_BUCKET}.duckdb"
os.makedirs("/app/data", exist_ok=True)


def duckdb_start_and_setup():
    """Blocking: Start DuckDB, install/load extensions, create secret and views, start UI."""
    if not (MINIO_PUBLIC_HOST and MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        print("Missing MinIO config; exiting.", file=sys.stderr)
        sys.exit(1)

    conn = duckdb.connect(DB_PATH)

    conn.execute("SET home_directory='/home/nobody';")
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

    for ext in ("httpfs", "aws", "ui"):
        try:
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
        except Exception as e:
            print(f"[ext] failed to install/load {ext}: {e}")

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

    # set UI port and start the UI server
    try:
        conn.execute(f"SET ui_local_port = {UI_PORT};")
    except Exception as e:
        print("[ui] failed to set ui_local_port:", e)
    try:
        conn.execute("CALL start_ui_server();")
        print(f"[ui] Called start_ui_server() for localhost:{UI_PORT}")
    except Exception as e:
        print("[ui] start_ui_server() failed:", e)
        traceback.print_exc()
        sys.exit(1)


def wait_for_tcp(host: str, port: int, timeout: float) -> bool:
    """Wait until a TCP connect to (host,port) succeeds or timeout; returns True if success."""
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except Exception as e:
            last_exc = e
            time.sleep(0.5)
    print(
        f"[wait] TCP connect to {host}:{port} failed after {timeout}s, last error: {last_exc}"
    )
    return False


def prewarm_local_ui(
    url: str,
    timeout_per_try: float = PREWARM_TRY_TIMEOUT,
    max_total: float = PREWARM_TOTAL,
) -> bool:
    """Try small local GETs to let the UI fetch remote assets. Return True on some bytes received."""
    import urllib.request
    from urllib.error import URLError, HTTPError

    deadline = time.time() + max_total
    last_err = None
    headers = {"User-Agent": "duckdb-ui-prewarm/1.0"}
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=headers), timeout=timeout_per_try
            ) as resp:
                chunk = resp.read(1)
                print(
                    f"[prewarm] got response status={resp.status}, first-byte-exists={bool(chunk)}"
                )
                return True
        except (HTTPError, URLError, TimeoutError, ConnectionResetError) as e:
            last_err = e
            time.sleep(0.5)
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    print(f"[prewarm] failed within {max_total}s. last error: {last_err}")
    return False


# Shared state to indicate backend readiness
_backend_ready = False


async def proxy_handler(request):
    """Proxy incoming request to local DuckDB UI if backend ready; otherwise return 503."""
    global _backend_ready
    if not _backend_ready:
        # quick fail instead of hanging the client / edge
        retry_after = 5  # seconds
        return web.Response(
            status=503,
            text="Service unavailable: DuckDB UI not ready yet. Retry later.",
            headers={"Retry-After": str(retry_after)},
        )

    target_url = f"http://127.0.0.1:{UI_PORT}{request.rel_url}"
    timeout = ClientTimeout(
        total=None,
        sock_connect=UPSTREAM_CONNECT_TIMEOUT,
        sock_read=UPSTREAM_READ_TIMEOUT,
    )
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
            # if connection refused or other errors, mark backend not ready and return 502
            print(f"[proxy] upstream error: {e}")
            _backend_ready = False
            return web.Response(status=502, text=f"Bad gateway: {e}")


async def health_handler(request):
    return web.Response(text="ok", status=200)


async def main():
    global _backend_ready

    loop = asyncio.get_running_loop()
    # 1) Blocking setup of DuckDB and CALL start_ui_server()
    await loop.run_in_executor(None, duckdb_start_and_setup)

    # 2) Wait for TCP port to accept connections (the UI must bind)
    print(
        f"[wait] waiting up to {BACKEND_TCP_WAIT}s for localhost:{UI_PORT} to accept TCP"
    )
    ok = wait_for_tcp("127.0.0.1", UI_PORT, BACKEND_TCP_WAIT)
    if not ok:
        print(
            "[wait] backend TCP not ready after wait; proxy will still start but will return 503 until backend becomes available."
        )
        _backend_ready = False
    else:
        # 3) Pre-warm the UI (optional) — try to fetch first bytes so remote assets finish fetching
        local_root = f"http://127.0.0.1:{UI_PORT}/"
        print(
            f"[prewarm] attempting to prewarm local UI at {local_root} (timeout {PREWARM_TOTAL}s)"
        )
        pre_ok = prewarm_local_ui(
            local_root, timeout_per_try=PREWARM_TRY_TIMEOUT, max_total=PREWARM_TOTAL
        )
        if not pre_ok:
            print(
                "[prewarm] prewarm failed; the UI may still be cold on first external requests."
            )
        _backend_ready = True  # mark ready regardless — if prewarm failed, the proxy can still attempt requests

    # Start aiohttp proxy
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

    # Background monitor: if backend not ready yet, keep trying to detect it and flip _backend_ready true
    async def monitor_backend():
        global _backend_ready
        while True:
            if not _backend_ready:
                if wait_for_tcp("127.0.0.1", UI_PORT, 1.0):
                    print("[monitor] backend became available; marking ready")
                    _backend_ready = True
            await asyncio.sleep(1.0)

    monitor_task = asyncio.create_task(monitor_backend())

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        monitor_task.cancel()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted, exiting.")
    except Exception:
        traceback.print_exc()
        sys.exit(1)
