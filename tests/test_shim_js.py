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
        const _unloadHandlers = {};
        globalThis.window = {
          location: {
            pathname: %(page_path)s,
            origin: 'https://host.example',
            host: 'host.example',
            protocol: 'https:',
          },
          addEventListener(type, fn) { _unloadHandlers[type] = fn; },
          removeEventListener(type, fn) {
            if (_unloadHandlers[type] === fn) delete _unloadHandlers[type];
          },
        };
        // navigator is a read-only global in modern Node; override its
        // sendBeacon rather than reassigning the whole object.
        Object.defineProperty(globalThis, 'navigator', {
          value: { sendBeacon(url) { captured.beaconUrl = url; return true; } },
          configurable: true,
        });
        // crypto.randomUUID is a native Node global; the assertions below
        // match on a UUID-shaped value rather than a fixed string.
        // A WebSocket that immediately triggers the 1006 fallback path.
        globalThis.WebSocket = class {
          constructor(u) { captured.nativeWsUrl = u; this.readyState = 0;
            setTimeout(() => { this.readyState = 3;
              if (this.onclose) this.onclose({ code: 1006, reason: '', wasClean: false });
            }, 0);
          }
          close() {}
        };
        globalThis.WebSocket.CONNECTING = 0; globalThis.WebSocket.OPEN = 1;
        globalThis.WebSocket.CLOSING = 2; globalThis.WebSocket.CLOSED = 3;
        // The shim captures OriginalWebSocket = window.WebSocket; in a real
        // browser window === globalThis, so mirror the stub onto window.
        globalThis.window.WebSocket = globalThis.WebSocket;
        globalThis.EventSource = class {
          constructor(url) { captured.sseUrl = url; this.readyState = 0; }
          addEventListener() {} close() {}
        };
        globalThis.EventSource.CLOSED = 2;
        // CloseEvent is a universal browser global but only became a Node
        // global in v23+. Stub it so the harness runs on older Node (CI).
        if (typeof CloseEvent === 'undefined') {
          globalThis.CloseEvent = class CloseEvent extends Event {
            constructor(type, init = {}) {
              super(type, init);
              this.code = init.code; this.reason = init.reason;
              this.wasClean = init.wasClean;
            }
          };
        }
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
          // Fire a page-unload BEFORE close() so the sendBeacon path runs
          // while the SSE connection is still active (the reload scenario).
          if (_unloadHandlers.pagehide) _unloadHandlers.pagehide();
          captured.unloadHandlerRegistered = !!_unloadHandlers.pagehide;
          ws.close();                // exercise the close URL
          captured.unloadHandlerCleared = !_unloadHandlers.pagehide;
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


def test_native_ws_url_rewritten_onto_page_prefix():
    # The native WebSocket attempt must target the PAGE's mount prefix, not the
    # host/prefix the app baked into its WS URL. marimo with no --base-url
    # builds a ROOT url (wss://host/ws) that ignores the sub-path mount; through
    # a sub-path proxy that never reaches the app (→ 1006). The shim must
    # rewrite it onto the page prefix so the native attempt actually connects.
    prefix = "/jupyterlab/default/proxy/2719"
    cap = _run_shim(
        prefix + "/",
        "wss://host.example/ws?session_id=abc",  # ROOT url, wrong prefix
    )
    # Native WS is dialed at the page's prefix, preserving the query.
    assert cap["nativeWsUrl"] == (
        "wss://host.example" + prefix + "/ws?session_id=abc"
    )


def test_native_ws_url_root_mount():
    # At the server root the native URL is unchanged.
    cap = _run_shim("/", "wss://host.example/ws?session_id=abc")
    assert cap["nativeWsUrl"] == "wss://host.example/ws?session_id=abc"


def test_close_on_unload_beacon():
    # On page unload (reload/navigation) the shim must sendBeacon a close for
    # its __wss_id, so the upstream session is torn down before the reloaded
    # page connects with a new session id. Without this, marimo demotes the
    # reconnecting page to a read-only kiosk view (blank notebook).
    prefix = "/jupyterlab/default/proxy/2719"
    cap = _run_shim(
        prefix + "/",
        "wss://host.example" + prefix + "/ws?session_id=abc",
    )
    assert cap["unloadHandlerRegistered"] is True
    assert cap["beaconUrl"].startswith(prefix + "/__wss/close?__wss_id=")
    # The beacon and the explicit close() must target the same shim id.
    beacon_id = cap["beaconUrl"].split("__wss_id=")[1]
    close_id = cap["closeUrl"].split("__wss_id=")[1]
    assert beacon_id == close_id
    # close() must unregister the handler to avoid leaking listeners.
    assert cap["unloadHandlerCleared"] is True
