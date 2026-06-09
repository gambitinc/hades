# Static server for the hades site with one dynamic touch: /install.sh is
# templated per-request so the script knows the origin that served it (the
# trycloudflare URL changes across tunnel restarts, so it can't be baked at
# build time). That lets the installer fetch /hades-src.tar.gz from the same
# place the user copied the command from.
import http.server
import socketserver


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/install.sh", "/cli.sh"):
            host = self.headers.get("Host", "localhost:8000")
            proto = self.headers.get("X-Forwarded-Proto") or (
                "https" if host.endswith(".trycloudflare.com") else "http"
            )
            origin = f"{proto}://{host}"
            with open(path.lstrip("/"), "rb") as f:
                body = f.read().replace(b"__ORIGIN__", origin.encode())
            self.send_response(200)
            self.send_header("Content-Type", "text/x-shellscript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()


socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("0.0.0.0", 8000), Handler) as srv:
    srv.serve_forever()
