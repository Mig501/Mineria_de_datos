# ---------------- socket_source.py ----------------
import socket, json
from datetime import datetime, timezone

class SocketTickSource:
    def __init__(self, host="localhost", port=8080, timeout=10.0):
        self.host = host
        self.port = port
        self.timeout = timeout

    def iter_ticks(self, max_rows=None):
        rows = 0
        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            with sock.makefile("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    obj["_ingest_ts"] = datetime.now(timezone.utc)
                    yield obj
                    rows += 1
                    if max_rows and rows >= max_rows:
                        break
