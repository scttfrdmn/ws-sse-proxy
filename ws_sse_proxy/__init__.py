"""ws-sse-proxy: WebSocket-to-SSE translation proxy.

A drop-in reverse proxy for web applications that require WebSocket, deployed
in environments where WebSocket connections are blocked or dropped (AWS ALB,
corporate proxies, reverse proxies that strip Upgrade headers, etc.).

The proxy sits in front of your application and:
- Passes all HTTP traffic through unchanged
- Injects a small JavaScript shim that intercepts WebSocket connections
- If WebSocket works, the shim stays out of the way (zero overhead)
- If WebSocket fails, it transparently falls back to SSE + HTTP POST
- Maintains a real WebSocket to your app on localhost where it works fine
"""

__version__ = "0.1.3"
