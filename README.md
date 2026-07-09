# ws-sse-proxy

[![PyPI version](https://img.shields.io/pypi/v/ws-sse-proxy)](https://pypi.org/project/ws-sse-proxy/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ws-sse-proxy)](https://pypi.org/project/ws-sse-proxy/)
[![conda-forge](https://img.shields.io/badge/conda--forge-pending-orange?logo=condaforge)](https://github.com/conda-forge/staged-recipes/pull/32681)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A drop-in reverse proxy that transparently translates WebSocket connections to SSE + HTTP POST, for environments where WebSocket is blocked.

## The Problem

Some deployment environments block or drop WebSocket connections: AWS ALBs that strip `Connection: Upgrade` headers, corporate proxies, reverse proxies with misconfigured WebSocket support, or platforms like AWS SageMaker Studio Lab where the gateway kills WebSocket on certain paths.

Your web application works fine on localhost but shows "connecting..." or blank content when deployed behind one of these proxies — because WebSocket never completes.

## How It Works

```
Browser → Broken Proxy → ws-sse-proxy (port 8081) → WebSocket → Your App (port 8080)
           (HTTP only)                                (localhost, works fine)
```

The proxy:

1. **Passes all HTTP through** to your application unchanged
2. **Injects a tiny JavaScript shim** into HTML responses that wraps `window.WebSocket`
3. The shim **tries real WebSocket first** — if it works, there's zero overhead
4. If WebSocket fails (code 1006 or connection stall), it **falls back to SSE + POST**
5. The `/__wss/events` endpoint opens a real WebSocket to your app on localhost and streams messages back as Server-Sent Events
6. The `/__wss/send` POST endpoint forwards client messages over the local WebSocket

Your application doesn't need any changes. The proxy handles the translation.

## Installation

```bash
pip install ws-sse-proxy
```

## Usage

```bash
# Your app is running on port 8080
ws-sse-proxy --target-port 8080 --listen-port 8081
```

Then point your users (or proxy/gateway) at port 8081 instead of 8080.

### All Options

```
ws-sse-proxy --target-port PORT --listen-port PORT [OPTIONS]

Required:
  --target-port PORT     Port your application is listening on
  --listen-port PORT     Port for the proxy to listen on

Optional:
  --target-host HOST     Target host (default: localhost)
  --host HOST            Bind address (default: 0.0.0.0)
  --log-level LEVEL      DEBUG, INFO, WARNING, or ERROR (default: INFO)
```

### As a Python module

```bash
python -m ws_sse_proxy --target-port 8080 --listen-port 8081
```

### Programmatic

```python
from ws_sse_proxy.proxy import create_proxy

app = create_proxy(target_port=8080)

import uvicorn
uvicorn.run(app, host="0.0.0.0", port=8081)
```

## Example: marimo on AWS SageMaker

[marimo](https://marimo.io) is a reactive Python notebook that requires WebSocket. On SageMaker Studio Lab, the gateway drops WebSocket on proxy paths. With ws-sse-proxy:

```bash
# Start marimo
marimo edit --host 0.0.0.0 --port 2718 --no-token --headless &

# Start the proxy in front of it
ws-sse-proxy --target-port 2718 --listen-port 2719
```

Access marimo at `/proxy/2719/` — the proxy translates WebSocket to SSE automatically.

See [aws-marimo-sagemaker](https://github.com/scttfrdmn/aws-marimo-sagemaker) for a complete setup.

## How Detection Works

The injected JavaScript doesn't blindly replace WebSocket. It:

1. Attempts a real WebSocket connection
2. Sets a 3-second timeout for stalled connections
3. If the WebSocket opens then immediately closes with code 1006 (abnormal closure — the signature of a proxy dropping the connection), falls back to SSE
4. If WebSocket connects normally, uses it with zero overhead

This makes the proxy safe to use everywhere. In environments where WebSocket works, the real WebSocket is used.

## Technical Details

The proxy uses `/__wss/` as its namespace for shim endpoints, chosen to avoid collisions with application routes:

- `/__wss/events` — SSE endpoint (server → client)
- `/__wss/send` — POST endpoint (client → server)
- `/__wss/close` — POST endpoint (cleanup)

All other paths are proxied to the target application. HTML responses get the JavaScript shim injected before the first `<script>` tag.

Dependencies: `starlette`, `websockets`, `httpx`, `uvicorn` — all pure Python, no compiled extensions required.

## Development

```bash
# Install with test extras (using uv)
uv pip install -e '.[test]'

# Run the test suite
pytest
```

The suite covers HTTP pass-through, JavaScript shim injection (including
gzip-encoded HTML), and the SSE↔WebSocket bridge end-to-end against a live
proxy+target stack — text and binary round-trips, query-parameter
forwarding, and connection cleanup.

## Changelog

### 0.1.2

- Fix ([#2](https://github.com/scttfrdmn/ws-sse-proxy/issues/2)): SSE
  fallback now works when the proxy is served under a sub-path prefix
  (jupyter-server-proxy, JupyterHub, SageMaker Studio). The injected shim
  previously assumed a root mount and requested `/__wss/*` at the wrong
  path, causing a 404 retry loop. It now derives the mount prefix from the
  page and WebSocket URLs and forwards the app-relative WebSocket path.

### 0.1.1

- Fix: query parameters forwarded to the target WebSocket are now
  percent-encoded. Values containing spaces, `&`, or `=` (e.g. auth tokens)
  previously corrupted the WebSocket handshake.
- Add: test suite.

### 0.1.0

- Initial release.

## License

MIT
