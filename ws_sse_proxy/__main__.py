"""Run ws-sse-proxy from the command line.

Usage:
    ws-sse-proxy --target-port 8080 --listen-port 8081
    python -m ws_sse_proxy --target-port 8080 --listen-port 8081
"""

import argparse
import logging
import sys


def main():
    parser = argparse.ArgumentParser(
        description=(
            "WebSocket-to-SSE translation proxy. Sits in front of a web "
            "application and transparently falls back to SSE + HTTP POST "
            "when WebSocket connections are blocked."
        ),
    )
    parser.add_argument(
        "--target-port",
        type=int,
        required=True,
        help="Port the target application is listening on",
    )
    parser.add_argument(
        "--target-host",
        default="localhost",
        help="Host the target application is listening on (default: localhost)",
    )
    parser.add_argument(
        "--listen-port",
        type=int,
        required=True,
        help="Port for the proxy to listen on",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    from .proxy import create_proxy

    app = create_proxy(
        target_port=args.target_port,
        target_host=args.target_host,
    )

    try:
        import uvicorn
    except ImportError:
        print(
            "uvicorn is required: pip install uvicorn",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"ws-sse-proxy v0.1.2")
    print(f"  Listening on {args.host}:{args.listen_port}")
    print(f"  Proxying to {args.target_host}:{args.target_port}")
    print(f"  WebSocket fallback: SSE + HTTP POST")
    print()

    uvicorn.run(
        app,
        host=args.host,
        port=args.listen_port,
        log_level=args.log_level.lower(),
    )


if __name__ == "__main__":
    main()
