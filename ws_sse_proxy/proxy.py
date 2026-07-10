"""WebSocket-to-SSE translation proxy.

A generic reverse proxy that translates WebSocket connections to
SSE (Server-Sent Events) + HTTP POST for environments where WebSocket
connections are blocked.

This module is application-agnostic — it doesn't know or care what's
behind the WebSocket. marimo, Streamlit, Bokeh, Dask dashboards, or
any other WebSocket-dependent app will work.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Optional
from urllib.parse import urlencode

import httpx
import websockets.asyncio.client
import websockets.exceptions
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from .shim_js import SHIM_SCRIPT

logger = logging.getLogger("ws_sse_proxy")

# How often to wake up while waiting for the next upstream frame to check
# whether the browser has disconnected. Small enough to tear the upstream
# session down promptly (so a reload isn't demoted to kiosk), large enough
# to avoid a busy loop on an idle connection. Doubles as the SSE keepalive
# interval: on each idle wakeup we emit a comment line so the stream keeps
# producing bytes.
DISCONNECT_POLL_SECONDS = 1.0

# Some reverse proxies (notably SageMaker Studio's jupyter-server-proxy, built
# on Tornado) buffer a streaming `text/event-stream` response and only flush
# to the client once an internal buffer fills. marimo's first event
# (`kernel-ready`) is small (~600 bytes), so it can sit in that buffer forever
# while the connection idles — the browser connects (HTTP 200) but renders a
# blank page because it never receives the event. `X-Accel-Buffering: no` only
# influences nginx, not Tornado. The fix is to (1) send a large comment
# preamble as the very first bytes so the proxy's buffer spills immediately,
# and (2) emit a periodic comment keepalive so the stream keeps flowing. SSE
# comment lines start with ':' and are ignored by EventSource, so they are
# invisible to the application.
SSE_PREAMBLE = ":" + (" " * 2048) + "\n\n"
SSE_KEEPALIVE = ": keepalive\n\n"


class ConnectionPool:
    """Manages active WebSocket connections keyed by shim ID."""

    def __init__(self):
        self._connections: dict[
            str, websockets.asyncio.client.ClientConnection
        ] = {}

    async def open(
        self, shim_id: str, ws_url: str
    ) -> websockets.asyncio.client.ClientConnection:
        # Close any prior connection for this shim_id before opening a new one.
        # An EventSource reconnect (e.g. the browser retrying a buffered/stalled
        # SSE stream) hits the SSE endpoint again with the same __wss_id; without
        # this, the previous upstream WebSocket would be orphaned — dropped from
        # the dict but left OPEN. For single-editor apps like marimo, a lingering
        # OPEN session keeps the "editor" role and demotes the reconnecting
        # client to read-only (kiosk), rendering a blank notebook.
        await self.close(shim_id)
        ws = await websockets.asyncio.client.connect(
            ws_url,
            additional_headers={"Origin": "http://localhost"},
            max_size=None,
        )
        self._connections[shim_id] = ws
        return ws

    def get(self, shim_id: str) -> Optional[
        websockets.asyncio.client.ClientConnection
    ]:
        return self._connections.get(shim_id)

    async def close(self, shim_id: str):
        ws = self._connections.pop(shim_id, None)
        if ws:
            try:
                await ws.close()
            except Exception:
                pass


def create_proxy(
    target_port: int,
    target_host: str = "localhost",
) -> Starlette:
    """Create the ASGI proxy application.

    Args:
        target_port: Port the target application is listening on.
        target_host: Host the target application is listening on.
                     Defaults to localhost.

    Returns:
        A Starlette ASGI application.
    """
    target_http = f"http://{target_host}:{target_port}"
    target_ws = f"ws://{target_host}:{target_port}"
    pool = ConnectionPool()
    # Shared HTTP client for proxying. trust_env=False prevents httpx
    # from picking up system proxy env vars (ALL_PROXY, HTTP_PROXY,
    # SOCKS proxy, etc.) — we're always connecting to a local target.
    http_client = httpx.AsyncClient(trust_env=False, timeout=30.0)

    async def sse_endpoint(request: Request) -> Response:
        """SSE endpoint: opens WebSocket to target, streams back as events."""
        shim_id = request.query_params.get("__wss_id")
        if not shim_id:
            return Response("Missing __wss_id", status_code=400)

        # Build the target WebSocket URL, forwarding query params
        # (but stripping the shim-internal __wss_* params)
        ws_path = request.query_params.get("__wss_path", "/ws")
        params = [
            (k, v)
            for k, v in request.query_params.multi_items()
            if not k.startswith("__wss_")
        ]
        qs = urlencode(params)
        ws_url = f"{target_ws}{ws_path}"
        if qs:
            ws_url += f"?{qs}"

        logger.info("SSE bridge: connecting to %s", ws_url)

        try:
            ws = await pool.open(shim_id, ws_url)
        except Exception as e:
            logger.error("Failed to connect to target WebSocket: %s", e)
            return Response(
                f"WebSocket connection failed: {e}", status_code=502
            )

        async def event_generator():
            # Forward each upstream WebSocket frame, but also poll for client
            # disconnect while parked waiting for the next frame. Without this,
            # when the SSE response is buffered by an intermediary (e.g.
            # SageMaker's jupyter-server-proxy) the server never observes the
            # browser going away: the generator blocks awaiting the next frame,
            # so the `finally` (and thus pool.close) never runs and the upstream
            # session leaks in the OPEN state — which is what demotes the next
            # connection to a read-only kiosk view. We keep a single persistent
            # recv() task (recreating it would drop frames) and wake up on a
            # short timeout to check request.is_disconnected().
            # Force the buffering proxy to spill immediately so the first real
            # event (marimo's kernel-ready) reaches the browser without waiting
            # for more bytes.
            yield SSE_PREAMBLE
            recv_task = asyncio.ensure_future(ws.recv())
            try:
                while True:
                    done, _ = await asyncio.wait(
                        {recv_task}, timeout=DISCONNECT_POLL_SECONDS
                    )
                    if recv_task not in done:
                        # Timed out waiting for a frame; check for disconnect.
                        if await request.is_disconnected():
                            logger.info("Client disconnected for %s", shim_id)
                            break
                        # Still connected but idle: emit a keepalive so the
                        # stream keeps flowing (and any buffered bytes flush).
                        yield SSE_KEEPALIVE
                        continue
                    message = recv_task.result()
                    recv_task = asyncio.ensure_future(ws.recv())
                    if isinstance(message, bytes):
                        encoded = base64.b64encode(message).decode("ascii")
                        yield f"event: binary\ndata: {encoded}\n\n"
                    else:
                        lines = message.split("\n")
                        yield "data: " + "\ndata: ".join(lines) + "\n\n"
            except websockets.exceptions.ConnectionClosed:
                logger.info("Target WebSocket closed for %s", shim_id)
                yield "event: close\ndata: connection closed\n\n"
            except Exception as e:
                logger.error("SSE bridge error: %s", e)
                yield f"event: error\ndata: {e}\n\n"
            finally:
                recv_task.cancel()
                await pool.close(shim_id)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    async def send_endpoint(request: Request) -> Response:
        """Receive a message from the browser and forward to target WebSocket."""
        shim_id = request.query_params.get("__wss_id")
        if not shim_id:
            return Response("Missing __wss_id", status_code=400)

        ws = pool.get(shim_id)
        if not ws:
            return Response("No active connection", status_code=404)

        body = await request.body()
        try:
            # Determine text vs binary from Content-Type header, falling
            # back to UTF-8 decode attempt.  Most WebSocket messages from
            # web apps (JSON) are text frames; sending them as binary will
            # break servers that call receive_text().
            content_type = request.headers.get("content-type", "")
            if "octet-stream" in content_type:
                await ws.send(body)
            else:
                try:
                    await ws.send(body.decode("utf-8"))
                except UnicodeDecodeError:
                    await ws.send(body)
            return Response("OK", status_code=200)
        except Exception as e:
            logger.error("Failed to send to target: %s", e)
            return Response(f"Send failed: {e}", status_code=502)

    async def close_endpoint(request: Request) -> Response:
        """Close a shim connection."""
        shim_id = request.query_params.get("__wss_id")
        if shim_id:
            await pool.close(shim_id)
        return Response("OK", status_code=200)

    async def proxy_handler(request: Request) -> Response:
        """Reverse-proxy all other requests, injecting JS shim into HTML."""
        path = request.url.path
        query = str(request.url.query)
        target_url = f"{target_http}{path}"
        if query:
            target_url += f"?{query}"

        try:
            resp = await http_client.request(
                method=request.method,
                url=target_url,
                headers={
                    k: v
                    for k, v in request.headers.items()
                    if k.lower()
                    not in ("host", "connection", "transfer-encoding")
                },
                content=(
                    await request.body()
                    if request.method in ("POST", "PUT", "PATCH")
                    else None
                ),
                follow_redirects=False,
            )
        except httpx.ConnectError:
            return Response(
                "Target application not reachable", status_code=502
            )

        content_type = resp.headers.get("content-type", "")
        body = resp.content

        if "text/html" in content_type:
            html = body.decode("utf-8", errors="replace")
            injection_point = html.find("<script")
            if injection_point == -1:
                injection_point = html.find("</head>")
            if injection_point != -1:
                html = (
                    html[:injection_point]
                    + SHIM_SCRIPT
                    + html[injection_point:]
                )
            else:
                html = SHIM_SCRIPT + html
            body = html.encode("utf-8")

        excluded = {
            "transfer-encoding",
            "connection",
            "content-encoding",
            "content-length",
        }
        headers = {
            k: v
            for k, v in resp.headers.items()
            if k.lower() not in excluded
        }

        return Response(
            content=body,
            status_code=resp.status_code,
            headers=headers,
        )

    routes = [
        Route("/__wss/events", sse_endpoint),
        Route("/__wss/send", send_endpoint, methods=["POST"]),
        Route("/__wss/close", close_endpoint, methods=["POST"]),
        Route(
            "/{path:path}",
            proxy_handler,
            methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        ),
        Route("/", proxy_handler),
    ]

    return Starlette(routes=routes)
