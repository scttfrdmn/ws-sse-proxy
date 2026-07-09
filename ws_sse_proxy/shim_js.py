"""JavaScript WebSocket shim that falls back to SSE + HTTP POST.

This script is injected into HTML responses from the proxied application.
It monkey-patches window.WebSocket with a wrapper that:
1. Tries the real WebSocket first
2. If it fails (code 1006 or connection stall), falls back to SSE + POST
3. If WebSocket works, has zero overhead

The shim is completely transparent to the application — it implements
the full WebSocket API interface.
"""

SHIM_SCRIPT = """
<script>
(function() {
  const OriginalWebSocket = window.WebSocket;

  class ShimmedWebSocket extends EventTarget {
    constructor(url, protocols) {
      super();
      this.url = url;
      this.readyState = ShimmedWebSocket.CONNECTING;
      this.bufferedAmount = 0;
      this.extensions = '';
      this.protocol = '';
      this.binaryType = 'blob';

      // Extract query params from the WS URL
      const wsUrl = new URL(url, window.location.origin);
      this._queryString = wsUrl.search;

      // Derive the mount prefix (base path) so the /__wss/* endpoints resolve
      // correctly even when the app is served under a sub-path prefix
      // (jupyter-server-proxy, JupyterHub, SageMaker Studio, etc.). The page
      // and its WebSocket share that prefix, so the longest common path-segment
      // prefix of the two IS the mount. Stripping it from the WS path yields
      // the app-relative path the proxy needs as __wss_path.
      //   page: /jupyterlab/default/proxy/2719/     ws: /jupyterlab/.../2719/ws
      //   -> base = /jupyterlab/default/proxy/2719   targetWsPath = /ws
      // At the server root, base is '' and targetWsPath is the WS path as-is.
      const pageSegs = window.location.pathname.split('/');
      const wsSegs = wsUrl.pathname.split('/');
      const max = Math.min(pageSegs.length, wsSegs.length);
      const common = [];
      for (let i = 0; i < max && pageSegs[i] === wsSegs[i]; i++) {
        common.push(pageSegs[i]);
      }
      this._shimBasePath = common.join('/');
      this._targetWsPath =
        wsUrl.pathname.slice(this._shimBasePath.length) || '/';

      // Generate a unique ID for this connection
      this._shimId = crypto.randomUUID();

      // Try real WebSocket first, fall back to SSE if it fails
      this._tryRealWebSocket(url, protocols);
    }

    _tryRealWebSocket(url, protocols) {
      try {
        this._realWs = new OriginalWebSocket(url, protocols);
        this._realWs.binaryType = this.binaryType;

        const failTimeout = setTimeout(() => {
          if (this._realWs.readyState === OriginalWebSocket.CONNECTING) {
            console.log('[ws-sse-proxy] WebSocket stalled, falling back to SSE');
            this._realWs.close();
            this._startSSE();
          }
        }, 3000);

        this._realWs.onopen = (e) => {
          clearTimeout(failTimeout);
          this.readyState = ShimmedWebSocket.OPEN;
          this.dispatchEvent(new Event('open'));
          if (this.onopen) this.onopen(e);
        };

        this._realWs.onclose = (e) => {
          clearTimeout(failTimeout);
          // Code 1006 = abnormal closure (proxy/gateway dropping the connection)
          if (e.code === 1006 && !this._sseActive) {
            console.log('[ws-sse-proxy] WebSocket closed with 1006, falling back to SSE');
            this._startSSE();
            return;
          }
          if (!this._sseActive) {
            this.readyState = ShimmedWebSocket.CLOSED;
            const closeEvent = new CloseEvent('close', {
              code: e.code, reason: e.reason, wasClean: e.wasClean
            });
            this.dispatchEvent(closeEvent);
            if (this.onclose) this.onclose(closeEvent);
          }
        };

        this._realWs.onerror = (e) => {
          clearTimeout(failTimeout);
          if (!this._sseActive) {
            if (this._realWs.readyState !== OriginalWebSocket.OPEN) {
              console.log('[ws-sse-proxy] WebSocket error, will fall back to SSE');
              return;
            }
            this.dispatchEvent(new Event('error'));
            if (this.onerror) this.onerror(e);
          }
        };

        this._realWs.onmessage = (e) => {
          this.dispatchEvent(new MessageEvent('message', { data: e.data }));
          if (this.onmessage) this.onmessage(e);
        };
      } catch (err) {
        console.log('[ws-sse-proxy] WebSocket creation failed, falling back to SSE');
        this._startSSE();
      }
    }

    _startSSE() {
      this._sseActive = true;
      this.readyState = ShimmedWebSocket.CONNECTING;

      const sep = this._queryString ? '&' : '?';
      const sseUrl = `${this._shimBasePath}/__wss/events${this._queryString}${sep}__wss_id=${this._shimId}&__wss_path=${encodeURIComponent(this._targetWsPath)}`;

      this._eventSource = new EventSource(sseUrl);

      this._eventSource.onopen = () => {
        this.readyState = ShimmedWebSocket.OPEN;
        this.dispatchEvent(new Event('open'));
        if (this.onopen) this.onopen(new Event('open'));
      };

      this._eventSource.onmessage = (e) => {
        const msgEvent = new MessageEvent('message', { data: e.data });
        this.dispatchEvent(msgEvent);
        if (this.onmessage) this.onmessage(msgEvent);
      };

      this._eventSource.addEventListener('binary', (e) => {
        // Binary data arrives base64-encoded
        const binary = atob(e.data);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {
          bytes[i] = binary.charCodeAt(i);
        }
        const blob = new Blob([bytes]);
        const data = this.binaryType === 'arraybuffer' ? bytes.buffer : blob;
        const msgEvent = new MessageEvent('message', { data });
        this.dispatchEvent(msgEvent);
        if (this.onmessage) this.onmessage(msgEvent);
      });

      this._eventSource.onerror = (e) => {
        if (this._eventSource.readyState === EventSource.CLOSED) {
          this.readyState = ShimmedWebSocket.CLOSED;
          const closeEvent = new CloseEvent('close', {
            code: 1006, reason: 'SSE connection closed', wasClean: false
          });
          this.dispatchEvent(closeEvent);
          if (this.onclose) this.onclose(closeEvent);
        }
      };
    }

    send(data) {
      if (this._sseActive) {
        const sendUrl = `${this._shimBasePath}/__wss/send?__wss_id=${this._shimId}`;
        // Use text/plain for strings (JSON, etc.) so the proxy sends a
        // text frame.  Use octet-stream for binary (ArrayBuffer, Blob)
        // so the proxy sends a binary frame.
        const isBinary = data instanceof ArrayBuffer || data instanceof Blob;
        fetch(sendUrl, {
          method: 'POST',
          headers: { 'Content-Type': isBinary ? 'application/octet-stream' : 'text/plain; charset=utf-8' },
          body: data,
        }).catch(err => {
          console.error('[ws-sse-proxy] Failed to send message:', err);
        });
      } else if (this._realWs) {
        this._realWs.send(data);
      }
    }

    close(code, reason) {
      if (this._eventSource) {
        this._eventSource.close();
      }
      if (this._realWs) {
        try { this._realWs.close(code, reason); } catch(e) {}
      }
      this.readyState = ShimmedWebSocket.CLOSED;

      if (this._sseActive) {
        fetch(`${this._shimBasePath}/__wss/close?__wss_id=${this._shimId}`, {
          method: 'POST'
        }).catch(() => {});
      }

      const closeEvent = new CloseEvent('close', {
        code: code || 1000, reason: reason || '', wasClean: true
      });
      this.dispatchEvent(closeEvent);
      if (this.onclose) this.onclose(closeEvent);
    }
  }

  ShimmedWebSocket.CONNECTING = 0;
  ShimmedWebSocket.OPEN = 1;
  ShimmedWebSocket.CLOSING = 2;
  ShimmedWebSocket.CLOSED = 3;

  ShimmedWebSocket.prototype.onopen = null;
  ShimmedWebSocket.prototype.onclose = null;
  ShimmedWebSocket.prototype.onerror = null;
  ShimmedWebSocket.prototype.onmessage = null;

  window.WebSocket = ShimmedWebSocket;
  console.log('[ws-sse-proxy] WebSocket shim installed (SSE fallback enabled)');
})();
</script>
"""
