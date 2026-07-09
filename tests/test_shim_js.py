"""Regression tests for the injected JS shim's URL construction.

The shim is browser JavaScript, so these run it under Node with a minimal
set of browser globals stubbed. They assert that the /__wss/* endpoints
resolve correctly both at the server root and — the regression from
issue #2 — when the app is served under a sub-path prefix
(jupyter-server-proxy / JupyterHub / SageMaker Studio).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import textwrap

import pytest

from ws_sse_proxy.shim_js import SHIM_SCRIPT

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not available"
)

# Strip the surrounding <script>...</script> wrapper to get runnable JS.
_SHIM_BODY = SHIM_SCRIPT.strip()
assert _SHIM_BODY.startswith("<script>") and _SHIM_BODY.endswith("</script>")
_SHIM_BODY = _SHIM_BODY[len("<script>") : -len("</script>")]


def _run_shim(page_path: str, ws_url: str) -> dict:
    """Instantiate ShimmedWebSocket in Node with browser globals stubbed,
    force the SSE fallback, and capture the URLs it builds."""
    harness = (
        """
        const captured = {};
        // --- minimal browser global stubs ---
        globalThis.window = {
          location: { pathname: %(page_path)s, origin: 'https://host.example' },
        };
        // crypto.randomUUID is a native Node global; the assertions below
        // match on a UUID-shaped value rather than a fixed string.
        // A WebSocket that immediately triggers the 1006 fallback path.
        globalThis.WebSocket = class {
          constructor(u) { this.readyState = 0;
            setTimeout(() => { this.readyState = 3;
              if (this.onclose) this.onclose({ code: 1006, reason: '', wasClean: false });
            }, 0);
          }
          close() {}
        };
        globalThis.WebSocket.CONNECTING = 0; globalThis.WebSocket.OPEN = 1;
        globalThis.WebSocket.CLOSING = 2; globalThis.WebSocket.CLOSED = 3;
        globalThis.EventSource = class {
          constructor(url) { captured.sseUrl = url; this.readyState = 0; }
          addEventListener() {} close() {}
        };
        globalThis.EventSource.CLOSED = 2;
        globalThis.fetch = (url, opts) => {
          if (url.includes('/__wss/send')) captured.sendUrl = url;
          if (url.includes('/__wss/close')) captured.closeUrl = url;
          return Promise.resolve({ ok: true });
        };
        // EventTarget / Event / MessageEvent / CloseEvent exist in Node 26 globally.

        // --- the shim under test ---
        %(shim)s

        // --- drive it ---
        const ws = new window.WebSocket(%(ws_url)s);
        setTimeout(() => {           // let the 1006 fallback fire -> _startSSE
          ws.send('hello');          // exercise the send URL
          ws.close();                // exercise the close URL
          console.log(JSON.stringify(captured));
        }, 20);
        """
        % {
            "page_path": json.dumps(page_path),
            "ws_url": json.dumps(ws_url),
            "shim": _SHIM_BODY,
        }
    )
    proc = subprocess.run(
        ["node", "--input-type=module", "-e", textwrap.dedent(harness)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _assert_urls(cap: dict, prefix: str):
    """The shim id is a real UUID, so match on structure around it."""
    assert cap["sseUrl"].startswith(
        prefix + "/__wss/events?session_id=abc&__wss_id="
    )
    # App-relative WS path is forwarded, not the full prefixed path.
    assert cap["sseUrl"].endswith("&__wss_path=%2Fws")
    assert cap["sendUrl"].startswith(prefix + "/__wss/send?__wss_id=")
    assert cap["closeUrl"].startswith(prefix + "/__wss/close?__wss_id=")


def test_root_mount_urls():
    # Served at the server root; WS at /ws. Base prefix is empty.
    cap = _run_shim("/", "wss://host.example/ws?session_id=abc")
    _assert_urls(cap, "")


def test_subpath_mount_urls_regression():
    # Issue #2: served under a jupyter-server-proxy / SageMaker prefix.
    # The page and its WebSocket share the mount prefix; the shim must
    # target <prefix>/__wss/* and pass the app-relative WS path (/ws),
    # NOT the full prefixed path (which produced the 404 loop).
    prefix = "/jupyterlab/default/proxy/2719"
    cap = _run_shim(
        prefix + "/",
        "wss://host.example" + prefix + "/ws?session_id=abc",
    )
    _assert_urls(cap, prefix)
