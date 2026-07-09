"""Shared fixtures for ws-sse-proxy tests.

Two kinds of targets are provided:

- ``proxy_client`` — an httpx client wired to the proxy ASGI app in-process
  (via ASGITransport). Good for HTTP pass-through and shim-injection tests
  that don't need the WebSocket bridge.

- ``live_stack`` — a real target server (uvicorn in a thread) with a
  WebSocket echo endpoint plus HTML/JSON routes, and a proxy server in
  front of it, both on ephemeral ports. Needed for the SSE bridge tests,
  because the proxy opens a real ``websockets`` client connection to the
  target — an in-process ASGI transport can't satisfy that.
"""

from __future__ import annotations

import socket
import threading
import time
from contextlib import closing

import httpx
import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    Response,
)
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from ws_sse_proxy.proxy import create_proxy


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def build_target_app() -> Starlette:
    """A representative WebSocket app: HTML page, JSON route, WS echo."""

    async def index(request):
        # Two <script> tags; shim should land before the first.
        return HTMLResponse(
            "<html><head><title>t</title></head>"
            "<body><script>window.first=1</script>"
            "<script>window.second=2</script></body></html>"
        )

    async def api(request):
        return JSONResponse({"ok": True, "echo": request.query_params.get("q")})

    async def plain(request):
        return PlainTextResponse("no html here")

    async def gzipped(request):
        import gzip

        html = (
            b"<html><head></head><body><script>window.g=1</script>"
            b"</body></html>"
        )
        return Response(
            gzip.compress(html),
            media_type="text/html",
            headers={"Content-Encoding": "gzip"},
        )

    async def ws_echo(ws: WebSocket):
        await ws.accept()
        # First frame reports the raw query string the target actually saw,
        # so tests can assert the proxy forwarded/encoded params correctly.
        await ws.send_text("qs:" + ws.url.query)
        try:
            while True:
                msg = await ws.receive()
                if msg["type"] == "websocket.disconnect":
                    break
                if msg.get("text") is not None:
                    # Echo text with a marker so we can assert the frame type.
                    await ws.send_text("text:" + msg["text"])
                elif msg.get("bytes") is not None:
                    await ws.send_bytes(b"bin:" + msg["bytes"])
        except WebSocketDisconnect:
            pass

    return Starlette(
        routes=[
            Route("/", index),
            Route("/api", api),
            Route("/plain", plain),
            Route("/gzipped", gzipped),
            WebSocketRoute("/ws", ws_echo),
        ]
    )


class _ThreadedServer:
    """Run a uvicorn server in a background thread on an ephemeral port."""

    def __init__(self, app, port: int):
        self.port = port
        config = uvicorn.Config(
            app, host="127.0.0.1", port=port, log_level="warning"
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self):
        self.thread.start()
        # Wait until the server reports it's up (bounded).
        for _ in range(100):
            if self.server.started:
                return
            time.sleep(0.05)
        raise RuntimeError("server did not start in time")

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=5)


@pytest.fixture
def proxy_app():
    """Proxy app pointed at a nonexistent target (for pure ASGI-level tests
    that stub the target separately or only exercise the /__wss error paths)."""
    return create_proxy(target_port=1, target_host="127.0.0.1")


@pytest.fixture
async def proxy_client(proxy_app):
    transport = httpx.ASGITransport(app=proxy_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        yield client


@pytest.fixture
def live_stack():
    """Start a real target server and a real proxy in front of it.

    Yields (proxy_base_url, target_base_url).
    """
    target_port = _free_port()
    proxy_port = _free_port()

    target = _ThreadedServer(build_target_app(), target_port)
    proxy = _ThreadedServer(
        create_proxy(target_port=target_port, target_host="127.0.0.1"),
        proxy_port,
    )
    target.start()
    proxy.start()
    try:
        yield (
            f"http://127.0.0.1:{proxy_port}",
            f"http://127.0.0.1:{target_port}",
        )
    finally:
        proxy.stop()
        target.stop()
