#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse
import signal
import sys
import threading


UPSTREAM_ROOT = (
    b"""<!DOCTYPE html><html><body>"""
    b"""<a href="cu130/">cu130</a>"""
    b"""<a href="torch/">torch</a>"""
    b"""</body></html>"""
)
UPSTREAM_CU130 = b"""<!DOCTYPE html><html><body><a href="torch/">torch</a></body></html>"""
UPSTREAM_TORCH = (
    b"""<!DOCTYPE html><html><body>torch cached directory index"""
    b"""<a href="torch-0.0.1.whl">torch</a></body></html>"""
)
UPSTREAM_CU130_TORCH = (
    b"""<!DOCTYPE html><html><body>cu130 torch cached directory index"""
    b"""<a href="torch-0.0.1+cu130.whl">torch</a></body></html>"""
)

objects = {"sentinel": b"0"}
objects_lock = threading.Lock()


class QuietHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format, *_args):
        return

    def send_bytes(self, status, body=b"", headers=None):
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)


class UpstreamHandler(QuietHandler):
    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            self.send_bytes(200, b"ok")
        elif path in ("/whl", "/whl/"):
            self.send_bytes(200, UPSTREAM_ROOT, {"Content-Type": "text/html"})
        elif path in ("/whl/cu130", "/whl/cu130/"):
            self.send_bytes(200, UPSTREAM_CU130, {"Content-Type": "text/html"})
        elif path in ("/whl/torch", "/whl/torch/"):
            self.send_bytes(200, UPSTREAM_TORCH, {"Content-Type": "text/html"})
        elif path in ("/whl/cu130/torch", "/whl/cu130/torch/"):
            self.send_bytes(200, UPSTREAM_CU130_TORCH, {"Content-Type": "text/html"})
        else:
            self.send_bytes(404, b"not found")


class S3Handler(QuietHandler):
    def do_HEAD(self):
        self.send_object(head=True)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            self.send_bytes(200, b"ok")
            return

        if path in ("/bucket", "/bucket/"):
            self.send_bytes(
                200,
                b'<?xml version="1.0"?><ListBucketResult/>',
                {"Content-Type": "application/xml"},
            )
            return

        self.send_object(head=False)

    def do_PUT(self):
        path = urlparse(self.path).path
        if not path.startswith("/bucket/"):
            self.send_bytes(404, b"not found")
            return

        key = unquote(path.removeprefix("/bucket/"))
        body = self.read_request_body()
        with objects_lock:
            objects[key] = body

        self.send_bytes(200, b"", {"ETag": '"fake-etag"'})

    def send_object(self, head):
        path = urlparse(self.path).path
        if not path.startswith("/bucket/"):
            self.send_bytes(404, b"not found")
            return

        key = unquote(path.removeprefix("/bucket/"))
        with objects_lock:
            body = objects.get(key)

        if body is None:
            self.send_bytes(404, b"not found")
            return

        headers = {"Content-Type": "text/html"}
        if self.headers.get("Range") == "bytes=0-0" and body:
            headers["Content-Range"] = f"bytes 0-0/{len(body)}"
            self.send_bytes(206, body[:1], headers)
            return

        self.send_bytes(200, b"" if head else body, headers)

    def read_request_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size_line = self.rfile.readline().split(b";", 1)[0].strip()
                size = int(size_line, 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)
            return b"".join(chunks)

        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length)


def serve(port, handler):
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def main():
    upstream = serve(18080, UpstreamHandler)
    s3 = serve(18081, S3Handler)

    def shutdown(_signum, _frame):
        upstream.shutdown()
        s3.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    print("fake services ready", flush=True)
    signal.pause()


if __name__ == "__main__":
    main()
