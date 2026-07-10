"""JavaScript WebSocket shim that falls back to SSE + HTTP POST.

This script is injected into HTML responses from the proxied application.
It monkey-patches window.WebSocket with a wrapper that:
1. Tries the real WebSocket first
2. If it fails (code 1006 or connection stall), falls back to SSE + POST
3. If WebSocket works, has zero overhead

The shim is completely transparent to the application — it implements
the full WebSocket API interface.
"""

SHIM_SCRIPT = r"""
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

      // Derive the mount prefix (base path) under which the page is served, so
      // both the native WebSocket and the /__wss/* fallback endpoints resolve
      // correctly when the app is behind a sub-path prefix (jupyter-server-
      // proxy, JupyterHub, SageMaker Studio, etc.).
      //
      // We must NOT trust the host/prefix in the app's WS URL: some apps
      // (e.g. marimo with no --base-url) build it from an empty base, yielding
      // a root URL like wss://host/ws that ignores the mount prefix entirely.
      // Through a sub-path proxy that URL never reaches the app (→ 1006). The
      // reliable prefix is the PAGE's own directory: everything up to and
      // including the last '/' of window.location.pathname.
      //   page /jupyterlab/default/proxy/2719/  -> base /jupyterlab/default/proxy/2719
      //   page /                                -> base ''
      const pagePath = window.location.pathname;
      this._shimBasePath = pagePath.replace(/\/[^/]*$/, '');

      // The app-relative WS path is the app's WS pathname with any leading
      // prefix it happens to share with the page stripped, so double-prefixing
      // (e.g. a base-url that already includes the mount) collapses correctly.
      let appWsPath = wsUrl.pathname;
      if (this._shimBasePath && appWsPath.startsWith(this._shimBasePath)) {
        appWsPath = appWsPath.slice(this._shimBasePath.length);
      }
      this._targetWsPath = appWsPath || '/';

      // The corrected native WebSocket URL: page origin + mount prefix + the
      // app-relative WS path + original query. This is what the browser should
      // actually dial, regardless of what host/prefix the app put in `url`.
      const wsScheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      this._nativeWsUrl = `${wsScheme}//${window.location.host}` +
        `${this._shimBasePath}${this._targetWsPath}${this._queryString}`;

      // Generate a unique ID for this connection
      this._shimId = crypto.randomUUID();

      // Try real WebSocket first, fall back to SSE if it fails
      this._tryRealWebSocket(this._nativeWsUrl, protocols);
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

      // Tear down the upstream WebSocket when the page goes away (reload, tab
      // close, navigation). A normal fetch() is cancelled by the navigation
      // before it is sent, so we use sendBeacon, which the browser guarantees
      // to deliver during unload. This matters for single-editor apps like
      // marimo: on reload the page generates a NEW session id and connects
      // while the OLD session is still OPEN, which the server demotes to a
      // read-only (kiosk) view — a blank notebook. Closing the old session
      // first lets the reloaded page resume as the editor. Registered once per
      // SSE-active connection; the handler no-ops after close() clears the id.
      if (!this._unloadHandler && typeof window.addEventListener === 'function') {
        this._unloadHandler = () => {
          if (this._sseActive && this._shimId &&
              typeof navigator !== 'undefined' && navigator.sendBeacon) {
            navigator.sendBeacon(
              `${this._shimBasePath}/__wss/close?__wss_id=${this._shimId}`
            );
          }
        };
        window.addEventListener('pagehide', this._unloadHandler);
      }

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
      if (this._unloadHandler) {
        window.removeEventListener('pagehide', this._unloadHandler);
        this._unloadHandler = null;
      }
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
