"""Read API for dashboards and personalization. Stands in for the
dashboard API (ClickHouse reads) and the decision API (Redis reads).

Every query is scoped by tenant in the URL and in the SQL. There is no
endpoint that reads across tenants.

  GET /v1/tenants/{t}/live                 per-minute counts by type (humans only)
  GET /v1/tenants/{t}/visitors/{id}        identity, linked ids, server-side counts, segments
  GET /v1/tenants/{t}/segments/viewed_pricing?min=3
  GET /v1/tenants/{t}/events/{event_id}    is this event queryable yet (freshness probe)
  GET /v1/lag                              log head vs committed offset per partition
"""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from . import store
from .log import head_from_disk
from .processor import partitions


def canonical_ids(conn, tenant, any_id):
    """All ids that belong to one person inside one tenant (anon <-> user)."""
    ids, frontier = {any_id}, [any_id]
    while frontier:
        x = frontier.pop()
        for r in conn.execute("SELECT anonymous_id, user_id FROM identity WHERE tenant_id=? "
                              "AND (anonymous_id=? OR user_id=?)", (tenant, x, x)):
            for v in (r["anonymous_id"], r["user_id"]):
                if v and v not in ids:
                    ids.add(v)
                    frontier.append(v)
    return ids


def live(conn, tenant):
    rows = conn.execute("SELECT minute, type, n FROM rollup_minute WHERE tenant_id=? AND n<>0 "
                        "ORDER BY minute, type", (tenant,)).fetchall()
    return {"tenant_id": tenant, "minutes": [dict(r) for r in rows],
            "total_human_events": sum(r["n"] for r in rows),
            "excluded_bot_events": conn.execute("SELECT COUNT(*) FROM events WHERE tenant_id=? AND is_bot=1",
                                                (tenant,)).fetchone()[0]}


def visitor(conn, tenant, vid):
    ids = canonical_ids(conn, tenant, vid)
    marks = ",".join("?" * len(ids))
    q = (f"SELECT event_id, type, path, event_time, is_bot FROM events WHERE tenant_id=? "
         f"AND (anonymous_id IN ({marks}) OR user_id IN ({marks})) ORDER BY event_time")
    evs = [dict(r) for r in conn.execute(q, (tenant, *ids, *ids))]
    pricing = [e["event_id"] for e in evs if e["type"] == "page_view" and e["path"] == "/pricing" and not e["is_bot"]]
    return {"tenant_id": tenant, "ids": sorted(ids), "events": evs,
            "server_pricing_views": len(pricing), "pricing_view_event_ids": pricing,
            "segments": {"viewed_pricing_3x": len(pricing) >= 3}}


def segment_viewed_pricing(conn, tenant, minimum):
    people = {}
    for r in conn.execute("SELECT anonymous_id, user_id, event_id FROM events WHERE tenant_id=? AND "
                          "type='page_view' AND path='/pricing' AND is_bot=0", (tenant,)):
        key = min(canonical_ids(conn, tenant, r["user_id"] or r["anonymous_id"]))
        people.setdefault(key, []).append(r["event_id"])
    return {"tenant_id": tenant, "min": minimum,
            "members": {k: v for k, v in people.items() if len(v) >= minimum},
            "below_threshold": {k: len(v) for k, v in people.items() if len(v) < minimum}}


def lag(conn, data_dir, n_partitions):
    names = partitions(n_partitions)
    heads = head_from_disk(os.path.join(data_dir, "log"), names)
    done = {r["partition"]: r["seq"] for r in conn.execute("SELECT partition, seq FROM offsets")}
    return {n: {"head": heads[n], "committed": done.get(n, 0), "lag": heads[n] - done.get(n, 0)} for n in names}


def make_handler(db_path, data_dir, n_partitions):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _send(self, code, body):
            data = json.dumps(body, indent=1).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            u = urlparse(self.path)
            parts = [p for p in u.path.split("/") if p]
            conn = store.connect(db_path, readonly=True)
            try:
                if parts == ["v1", "lag"]:
                    return self._send(200, lag(conn, data_dir, n_partitions))
                if len(parts) >= 4 and parts[:2] == ["v1", "tenants"]:
                    tenant = parts[2]
                    if parts[3] == "live":
                        return self._send(200, live(conn, tenant))
                    if parts[3] == "visitors" and len(parts) == 5:
                        return self._send(200, visitor(conn, tenant, parts[4]))
                    if parts[3:] == ["segments", "viewed_pricing"]:
                        m = int(parse_qs(u.query).get("min", ["3"])[0])
                        return self._send(200, segment_viewed_pricing(conn, tenant, m))
                    if parts[3] == "events" and len(parts) == 5:
                        r = conn.execute("SELECT event_id, is_bot, committed_ts FROM events WHERE tenant_id=? "
                                         "AND event_id=?", (tenant, parts[4])).fetchone()
                        return self._send(200 if r else 404, dict(r) if r else {"found": False})
                self._send(404, {"error": "not found"})
            finally:
                conn.close()

    return Handler


def build(data_dir, port=8081, n_partitions=4):
    db = os.path.join(data_dir, "store.db")
    store.connect(db).close()  # make sure schema exists
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(db, data_dir, n_partitions))
    server.daemon_threads = True
    return server
