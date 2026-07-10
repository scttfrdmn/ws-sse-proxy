"""End-to-end SSE<->WebSocket bridge tests against a live proxy+target."""

from __future__ import annotations

import asyncio
import base64

import httpx
import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _read_one_event(lines, timeout: float = 5.0) -> str:
    """Read from a persistent line iterator until one SSE event (blank line).

    ``lines`` must be a single ``resp.aiter_lines()`` iterator reused across
    calls — creating a fresh one per call would re-stream the consumed body.

    Comment-only frames (every line starts with ':', e.g. the anti-buffering
    preamble and keepalives) are skipped — they are ignored by EventSource and
    carry no application data, so callers asserting on real events shouldn't
    see them.
    """

    async def _pump():
        buf = ""
        async for line in lines:
            if line == "":
                # Blank line terminates the event.
                if buf and not all(
                    ln.startswith(":") for ln in buf.splitlines()
                ):
                    return buf
                # Comment-only frame (preamble/keepalive) — skip and continue.
                buf = ""
                continue
            buf += line + "\n"
        return buf

    return await asyncio.wait_for(_pump(), timeout=timeout)


async def test_missing_shim_id_returns_400(live_stack):
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        resp = await c.get(f"{proxy_base}/__wss/events")
    assert resp.status_code == 400


async def test_send_without_connection_returns_404(live_stack):
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        resp = await c.post(
            f"{proxy_base}/__wss/send",
            params={"__wss_id": "nope"},
            content=b"hi",
        )
    assert resp.status_code == 404


async def test_sse_starts_with_flush_preamble(live_stack):
    # The stream must begin with a large SSE comment preamble so a buffering
    # intermediary (e.g. SageMaker's jupyter-server-proxy) flushes immediately
    # and the first real event isn't stranded in its buffer. The preamble is a
    # comment line (starts with ':') and must be big enough to exceed a typical
    # proxy buffer threshold.
    proxy_base, _ = live_stack
    async with httpx.AsyncClient(timeout=10.0) as c:
        async with c.stream(
            "GET",
            f"{proxy_base}/__wss/events",
            params={"__wss_id": "test-preamble", "__wss_path": "/ws"},
        ) as sse:
            assert sse.status_code == 200
            lines = sse.aiter_lines()
            first = await asyncio.wait_for(anext(lines), timeout=5.0)
    assert first.startswith(":")
    assert len(first) >= 2000


async def test_text_roundtrip_through_sse(live_stack):
    proxy_base, _ = live_stack
    shim_id = "test-text-1"
    async with httpx.AsyncClient(timeout=10.0) as c:
        # Open the SSE bridge (which opens a WS to the target /ws echo).
        async with c.stream(
            "GET",
            f"{proxy_base}/__wss/events",
            params={"__wss_id": shim_id, "__wss_path": "/ws"},
        ) as sse:
            assert sse.status_code == 200
            assert "text/event-stream" in sse.headers["content-type"]
            lines = sse.aiter_lines()

            # Give the bridge a moment to register the connection.
            await asyncio.sleep(0.3)

            # First event is the target's query-string report (see conftest).
            await _read_one_event(lines)

            # Send a text frame via the POST endpoint.
            send = await c.post(
                f"{proxy_base}/__wss/send",
                params={"__wss_id": shim_id},
                content="hello".encode("utf-8"),
                headers={"Content-Type": "text/plain; charset=utf-8"},
            )
            assert send.status_code == 200

            event = await _read_one_event(lines)
            # Echo server prepends "text:"; SSE frames it as data: lines.
            assert "data: text:hello" in event


async def test_binary_roundtrip_through_sse(live_stack):
    proxy_base, _ = live_stack
    shim_id = "test-bin-1"
    async with httpx.AsyncClient(timeout=10.0) as c:
        async with c.stream(
            "GET",
            f"{proxy_base}/__wss/events",
            params={"__wss_id": shim_id, "__wss_path": "/ws"},
        ) as sse:
            assert sse.status_code == 200
            lines = sse.aiter_lines()
            await asyncio.sleep(0.3)

            # Consume the target's query-string report frame first.
            await _read_one_event(lines)

            await c.post(
                f"{proxy_base}/__wss/send",
                params={"__wss_id": shim_id},
                content=b"\x00\x01\x02",
                headers={"Content-Type": "application/octet-stream"},
            )

            event = await _read_one_event(lines)
            # Binary echo "bin:\x00\x01\x02" arrives as event: binary + base64.
            assert "event: binary" in event
            data_line = [
                l for l in event.splitlines() if l.startswith("data: ")
            ][0]
            decoded = base64.b64decode(data_line[len("data: ") :])
            assert decoded == b"bin:\x00\x01\x02"


async def test_query_params_forwarded_and_encoded(live_stack):
    # A param value with characters that would corrupt a naive f"{k}={v}"
    # join (space, ampersand, equals) must survive to the target intact.
    proxy_base, _ = live_stack
    shim_id = "test-qs-1"
    tricky = "a b&c=d"
    async with httpx.AsyncClient(timeout=10.0) as c:
        async with c.stream(
            "GET",
            f"{proxy_base}/__wss/events",
            params={
                "__wss_id": shim_id,
                "__wss_path": "/ws",
                "token": tricky,
            },
        ) as sse:
            assert sse.status_code == 200
            lines = sse.aiter_lines()
            await asyncio.sleep(0.3)
            report = await _read_one_event(lines)

    # Target echoes its raw query string as "qs:<query>". The proxy must
    # have percent-encoded the value and dropped the __wss_* params.
    assert "qs:" in report
    qs = report.split("qs:", 1)[1].strip()
    assert "__wss_" not in qs
    # urllib.parse.parse_qs would round-trip the tricky value exactly.
    from urllib.parse import parse_qs

    parsed = parse_qs(qs)
    assert parsed.get("token") == [tricky]


async def test_abandoned_sse_client_is_cleaned_up(live_stack):
    # A client that drops the SSE stream without calling /__wss/close should
    # not leak: the target WS closes and a later /__wss/send returns 404
    # (connection no longer in the pool).
    proxy_base, _ = live_stack
    shim_id = "test-abandon-1"
    async with httpx.AsyncClient(timeout=10.0) as c:
        async with c.stream(
            "GET",
            f"{proxy_base}/__wss/events",
            params={"__wss_id": shim_id, "__wss_path": "/ws"},
        ) as sse:
            assert sse.status_code == 200
            lines = sse.aiter_lines()
            await asyncio.sleep(0.3)
            await _read_one_event(lines)  # qs report; connection is live now
        # Stream closed here (context exit) without an explicit /__wss/close.

        # Give the server's generator finally-block time to run cleanup.
        await asyncio.sleep(0.5)

        send = await c.post(
            f"{proxy_base}/__wss/send",
            params={"__wss_id": shim_id},
            content=b"late",
        )
    assert send.status_code == 404


async def test_close_endpoint_is_idempotent(live_stack):
    proxy_base, _ = live_stack
    async with httpx.AsyncClient() as c:
        # Closing an unknown id should still be OK (idempotent cleanup).
        resp = await c.post(
            f"{proxy_base}/__wss/close", params={"__wss_id": "ghost"}
        )
        assert resp.status_code == 200
        # And with no id at all.
        resp2 = await c.post(f"{proxy_base}/__wss/close")
        assert resp2.status_code == 200


async def test_reopen_same_shim_id_closes_prior_ws(live_stack):
    # An EventSource reconnect hits /__wss/events again with the SAME __wss_id.
    # The pool must close the prior upstream WebSocket before opening the new
    # one, otherwise the old socket is orphaned (dropped from the dict but left
    # OPEN). For single-editor apps like marimo, a lingering OPEN session keeps
    # the "editor" role and demotes the next page load to a read-only kiosk
    # (blank notebook). We assert the pool only ever tracks one live socket per
    # id, and that the new stream is fully functional (round-trips a frame).
    from ws_sse_proxy.proxy import ConnectionPool

    _, target_base = live_stack
    target_ws = target_base.replace("http://", "ws://") + "/ws"

    pool = ConnectionPool()
    ws1 = await pool.open("same-id", target_ws)
    ws2 = await pool.open("same-id", target_ws)
    await asyncio.sleep(0.2)

    assert ws1 is not ws2
    assert ws2.state.name == "OPEN"
    # Prior socket was closed by the close-before-open in pool.open().
    assert ws1.state.name in ("CLOSING", "CLOSED")
    # Only the latest socket is tracked.
    assert pool.get("same-id") is ws2

    await pool.close("same-id")
