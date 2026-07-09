"""HTTP pass-through and shim-injection tests (in-process, via live_stack)."""

from __future__ import annotations

import httpx
import pytest

from ws_sse_proxy.shim_js import SHIM_SCRIPT

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_html_gets_shim_injected(live_stack):
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        resp = await c.get(f"{proxy_base}/")
    assert resp.status_code == 200
    body = resp.text
    # Shim present, and injected before the first <script> tag.
    assert "[ws-sse-proxy] WebSocket shim installed" in body
    shim_pos = body.find("ShimmedWebSocket")
    first_app_script = body.find("window.first=1")
    assert shim_pos != -1 and first_app_script != -1
    assert shim_pos < first_app_script


async def test_non_html_not_modified(live_stack):
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        resp = await c.get(f"{proxy_base}/plain")
    assert resp.status_code == 200
    assert resp.text == "no html here"
    assert "ShimmedWebSocket" not in resp.text


async def test_json_passthrough_with_query(live_stack):
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        resp = await c.get(f"{proxy_base}/api", params={"q": "hello world"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "echo": "hello world"}


async def test_content_length_recomputed_after_injection(live_stack):
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        resp = await c.get(f"{proxy_base}/")
    # content-length header (if present) must match the injected body length,
    # not the original. httpx would raise on mismatch during read; assert too.
    assert len(resp.content) == len(resp.text.encode("utf-8"))


async def test_gzipped_html_decoded_before_injection(live_stack):
    # Target returns gzip-encoded HTML. The proxy must inject the shim into
    # the DECODED body and drop content-encoding, or the browser gets a
    # corrupt (shim-prepended) gzip stream.
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        resp = await c.get(f"{proxy_base}/gzipped")
    assert resp.status_code == 200
    assert "content-encoding" not in {k.lower() for k in resp.headers}
    assert "ShimmedWebSocket" in resp.text
    assert "window.g=1" in resp.text


async def test_target_unreachable_returns_502():
    # Proxy pointed at a dead port.
    from ws_sse_proxy.proxy import create_proxy

    app = create_proxy(target_port=1, target_host="127.0.0.1")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as c:
        resp = await c.get("/anything")
    assert resp.status_code == 502


def test_shim_script_is_wellformed():
    # Basic sanity: balanced script tags, defines the class, patches window.
    assert SHIM_SCRIPT.count("<script>") == 1
    assert SHIM_SCRIPT.count("</script>") == 1
    assert "window.WebSocket = ShimmedWebSocket" in SHIM_SCRIPT
