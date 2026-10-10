"""SDK-facing intake. Stands in for the ECS intake service behind the
existing endpoint in the AWS design.

Rules:
- The SDK contract does not change. POST /v1/events (one event or
  {"batch": [...]}) and the legacy path /track are both accepted.
- 202 is returned only after the record is fsynced to the durable log.
  A request that fails before that gets a 503, so the SDK retries
  (the fixture shows the SDK does retry: evt-0002 arrives twice).
- Backpressure is explicit: a bounded queue. When full, 503 + Retry-After,
  never a silent drop.
- Unparseable bodies are written to the quarantine partition (so they are
  counted) and answered 400 (a retry would fail the same way).
"""
import json
import os
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import schema
from .log import DurableLog, partition_for, QUARANTINE

MAX_BODY = 512 * 1024


class GroupCommitWriter(threading.Thread):
    """One thread owns the log. Requests enqueue records and wait; the
    writer drains whatever is queued, writes it, fsyncs once per partition,
    then wakes every waiter. Many requests share one fsync."""

    def __init__(self, log, max_queue=20000, max_batch=5000):
        super().__init__(daemon=True)
        self.log = log
        self.q = queue.Queue(maxsize=max_queue)
        self.max_batch = max_batch
        self.batches = 0
        self.records = 0

    def submit(self, items):
        done = threading.Event()
        slot = {"items": items, "done": done, "result": None, "error": None}
        try:
            self.q.put_nowait(slot)
        except queue.Full:
            return None  # caller answers 503
        done.wait(timeout=30)
        if not done.is_set() or slot["error"]:
            return None
        return slot["result"]

    def run(self):
        while True:
            first = self.q.get()
            slots = [first]
            n = len(first["items"])
            while n < self.max_batch:
                try:
                    s = self.q.get_nowait()
                except queue.Empty:
                    break
                slots.append(s)
                n += len(s["items"])
            flat = [it for s in slots for it in s["items"]]
            try:
                seqs = self.log.append_many(flat)
                i = 0
                for s in slots:
                    k = len(s["items"])
                    s["result"] = seqs[i:i + k]
                    i += k
            except Exception as exc:  # disk full, etc.: every waiter gets a 503
                for s in slots:
                    s["error"] = repr(exc)
            self.batches += 1
            self.records += len(flat)
            for s in slots:
                s["done"].set()


def make_handler(writer, n_partitions, preserve_received_at):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _send(self, code, body, extra=None):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/health":
                return self._send(200, {"ok": True, "queued": writer.q.qsize(),
                                        "fsync_batches": writer.batches, "records": writer.records})
            self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path not in ("/v1/events", "/track"):
                return self._send(404, {"error": "not found"})
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                return self._send(413, {"error": "body too large"})
            raw = self.rfile.read(length)
            src = self.headers.get("X-Source-Ref")  # replay tooling: which input row this is
            now = schema.now_utc()

            # one event or a batch
            bodies = None
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and isinstance(parsed.get("batch"), list):
                    bodies = [json.dumps(b).encode() for b in parsed["batch"]]
            except (ValueError, UnicodeDecodeError):
                pass
            if bodies is None:
                bodies = [raw]

            items, rejected = [], 0
            for i, b in enumerate(bodies):
                ok, rec = schema.intake_check(b, now, preserve_received_at)
                ref = src if len(bodies) == 1 else (f"{src}#{i}" if src else None)
                if ok:
                    if ref:
                        rec["_src"] = ref
                    part = str(partition_for(rec.get("tenant_id"), rec.get("anonymous_id"), n_partitions))
                    items.append((part, rec))
                else:
                    rejected += 1
                    text = b.decode("utf-8", "replace")
                    guess = None
                    if '"event_id":"' in text:
                        guess = text.split('"event_id":"', 1)[1].split('"', 1)[0]
                    q = {"raw": text, "reason": rec, "event_id_guess": guess,
                         "_ingest_ts": now.timestamp()}
                    if ref:
                        q["_src"] = ref
                    items.append((QUARANTINE, q))

            result = writer.submit(items)
            if result is None:
                return self._send(503, {"error": "overloaded, retry"}, {"Retry-After": "1"})
            seqs = [f"{p}:{s}" for p, s in result]
            if rejected and rejected == len(bodies):
                return self._send(400, {"error": "unparseable", "quarantined": seqs})
            return self._send(202, {"accepted": len(bodies) - rejected, "quarantined": rejected, "seqs": seqs})

    return Handler


def build(data_dir, port=8080, n_partitions=4, preserve_received_at=False):
    log = DurableLog(os.path.join(data_dir, "log"), n_partitions)
    writer = GroupCommitWriter(log)
    writer.start()
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(writer, n_partitions, preserve_received_at))
    server.daemon_threads = True
    server.request_queue_size = 1024
    server.pipeline_log = log
    return server


def serve(data_dir, port=8080, n_partitions=4, preserve_received_at=False):
    server = build(data_dir, port, n_partitions, preserve_received_at)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
        server.pipeline_log.close()
