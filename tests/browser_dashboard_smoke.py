"""Serve real-browser dashboard regressions without adding runtime dependencies.

Run with PYTHONPATH=src, then open the printed loopback URL. The harness embeds
the production HTML at desktop, tablet and 360px widths. All source records and
speech callbacks in this harness are explicitly synthetic test fixtures.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from ctmao_nsd.dashboard_v5 import DASHBOARD_HTML


ASSETS = Path(__file__).with_name("browser")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        route = self.path.split("?", 1)[0]
        if route == "/dashboard":
            page = DASHBOARD_HTML.replace(
                "<script>",
                '<script src="/fixtures.js"></script><script>',
                1,
            ).replace(
                "</body>", '<script src="/checks.js"></script></body>'
            )
            content, kind = page.encode(), "text/html"
        elif route in {"/fixtures.js", "/checks.js"}:
            content, kind = (ASSETS / route[1:]).read_bytes(), "text/javascript"
        elif route == "/":
            content, kind = (ASSETS / "dashboard.html").read_bytes(), "text/html"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", kind + "; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, *args: object) -> None:
        pass


if __name__ == "__main__":
    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        print(f"http://127.0.0.1:{server.server_port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
