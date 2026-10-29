#!/usr/bin/env python3
"""Serve the match feed: the broadcaster's events, released as the match reaches them.

    python3 tools/match_feed.py --port 28090 --speed 30 --start-at <epoch seconds>

GET /events?since=<seq> answers {"items": [...]} with every event released so
far whose seq is above the cursor, ascending, which is what clink's polling
source asks for: the cursor is exclusive, and the feed keeps everything it
has released, so a source that re-polls from a checkpointed cursor after a
restart reads exactly what it had not yet seen. GET /health answers once the
server is up.
"""
import argparse
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pacing  # noqa: E402

EVENTS = []
CLOCK = None


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        url = urlparse(self.path)
        if url.path == "/health":
            return self.reply(200, {"ok": True})
        if url.path != "/events":
            return self.reply(404, {"error": "no such path"})
        since = int(parse_qs(url.query).get("since", ["0"])[0] or 0)
        now = time.time()
        items = [e for e in EVENTS if e["seq"] > since and CLOCK.due_at(e["ts"]) <= now]
        return self.reply(200, {"items": items})

    def reply(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


class Clock(pacing.Clock):
    def due_at(self, ts_ms):
        return self.start + (ts_ms - self.first) / (1000.0 * self.speed)


def main():
    global CLOCK
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=28090)
    ap.add_argument("--speed", type=float, default=30.0)
    ap.add_argument("--start-at", type=float, default=None, help="wall time of the first record")
    args = ap.parse_args()
    m = pacing.manifest()
    EVENTS.extend(pacing.records("match.jsonl"))
    CLOCK = Clock(args.start_at or time.time(), m["first_ms"], args.speed)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"match_feed: {len(EVENTS)} events on port {args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
