"""Stream processor: reads the durable log and builds real-time state.

Stands in for the Lambda consumer on Kinesis in the AWS design.

Delivery guarantee, stated precisely:
- log -> processor is at-least-once (a crash replays from the last
  committed offset);
- processor -> store is effectively-once, because dedupe on
  (tenant_id, event_id) + content hash, the aggregates, the outcome ledger
  and the offset all commit in one transaction.

Every log record gets exactly one row in `outcomes`. That ledger is what
the reconciler checks: nothing is consumed without an explanation.
"""
import json
import os
import time

from . import schema, store
from .log import Tailer, QUARANTINE

BOT_BURST = 4            # page views from one visitor ...
BOT_WINDOW_S = 1.0       # ... inside this many seconds (server receive time) = automated
ROLLUP_TYPES = {"page_view", "click", "form_submit", "custom"}


def partitions(n):
    return [str(i) for i in range(n)] + [QUARANTINE]


def load_offsets(conn, names):
    got = {r["partition"]: r["seq"] for r in conn.execute("SELECT partition, seq FROM offsets")}
    return {n: got.get(n, 0) for n in names}


def _minute(iso):
    return iso[:16]  # YYYY-MM-DDTHH:MM


def _bump(conn, tenant, minute, etype, delta):
    conn.execute(
        "INSERT INTO rollup_minute(tenant_id, minute, type, n) VALUES(?,?,?,?) "
        "ON CONFLICT(tenant_id, minute, type) DO UPDATE SET n = n + excluded.n",
        (tenant, minute, etype, delta))


def _outcome(conn, part, seq, tenant, event_id, outcome, detail, src):
    conn.execute("INSERT INTO outcomes VALUES(?,?,?,?,?,?,?)",
                 (part, seq, tenant, event_id, outcome, detail, src))


def handle(conn, part, seq, rec, commit_ts):
    src = rec.get("_src")
    if part == QUARANTINE:
        conn.execute("INSERT INTO quarantine VALUES(?,?,?,?,?,?,?)",
                     (part, seq, None, rec.get("event_id_guess"), rec.get("reason"), rec.get("raw"), src))
        _outcome(conn, part, seq, None, rec.get("event_id_guess"), "quarantined", rec.get("reason"), src)
        return

    event, flags, reason = schema.normalize(rec)
    tenant, eid = rec.get("tenant_id"), rec.get("event_id")
    if reason:
        conn.execute("INSERT INTO quarantine VALUES(?,?,?,?,?,?,?)",
                     (part, seq, tenant, eid, reason, json.dumps(rec, sort_keys=True), src))
        _outcome(conn, part, seq, tenant, eid, "quarantined", reason, src)
        return

    chash = schema.content_hash(rec)
    existing = conn.execute("SELECT content_hash, src FROM events WHERE tenant_id=? AND event_id=?",
                            (tenant, eid)).fetchone()
    if existing is None and event["type"] == "privacy_request":
        existing = conn.execute("SELECT 'privacy' AS content_hash, src FROM privacy_requests "
                                "WHERE tenant_id=? AND request_event_id=?", (tenant, eid)).fetchone()
        if existing:
            _outcome(conn, part, seq, tenant, eid, "duplicate", f"same as {existing['src']}", src)
            return
    if existing is not None:
        if existing["content_hash"] == chash:
            _outcome(conn, part, seq, tenant, eid, "duplicate", f"retry of {existing['src']}", src)
        else:
            # Same id, different body: a client id collision, not a retry.
            # Neither copy is silently dropped; a person looks at it.
            conn.execute("INSERT INTO quarantine VALUES(?,?,?,?,?,?,?)",
                         (part, seq, tenant, eid, "event_id_collision", json.dumps(rec, sort_keys=True), src))
            _outcome(conn, part, seq, tenant, eid, "quarantined", "event_id_collision", src)
        return

    anon, user = event.get("anonymous_id"), event.get("user_id")
    received = schema.fmt_ts(schema.parse_ts(event["received_at"]))

    # A replay or backfill from the raw archive must not bring back a person
    # who was erased. Events received before the erasure are suppressed.
    for x in (anon, user):
        if x:
            row = conn.execute("SELECT erased_at FROM erased WHERE tenant_id=? AND id_hash=?",
                               (tenant, erased_hash(tenant, x))).fetchone()
            if row and received <= row["erased_at"]:
                _outcome(conn, part, seq, tenant, eid, "suppressed_erased", "subject erased before replay", src)
                return

    if event["type"] == "privacy_request":
        # Arrived through the public SDK write key, so anyone could have sent it.
        # Recorded and frozen for verification; NOT executed from the stream.
        props = event.get("properties") or {}
        conn.execute("INSERT INTO privacy_requests VALUES(?,?,?,?,?,?,?,?,?)",
                     (tenant, eid, user, anon, props.get("regulation"), "pending_verification",
                      received, None, src))
        # No identity link from an unverified request: anyone with the public
        # write key could otherwise tie any visitor to any user, and a later
        # real deletion would wipe the wrong person's data.
        _outcome(conn, part, seq, tenant, eid, "privacy_request_queued", ";".join(flags), src)
        return

    if anon and user:
        conn.execute("INSERT OR IGNORE INTO identity VALUES(?,?,?,?,?)",
                     (tenant, anon, user, event["type"], eid))

    props = event.get("properties") or {}
    ref = str(props.get("referrer") or "")
    if any(w in ref for w in ("bot", "scanner", "crawler", "spider")):
        flags.append("bot_like_referrer")  # a hint only; never excludes on its own

    is_bot = 1 if conn.execute("SELECT 1 FROM bots WHERE tenant_id=? AND anonymous_id=?",
                               (tenant, anon)).fetchone() else 0
    minute = _minute(event["event_time"])
    conn.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (tenant, eid, chash, anon, user, event["type"], event["event_time"], received,
                  minute, props.get("path"), json.dumps(props, sort_keys=True), ";".join(flags),
                  is_bot, rec.get("_ingest_ts"), commit_ts, src))
    if event["type"] in ROLLUP_TYPES and not is_bot:
        _bump(conn, tenant, minute, event["type"], 1)

    # Behavioral bot rule: a burst of page views from one visitor.
    if event["type"] == "page_view" and not is_bot and anon:
        window_start = schema.fmt_ts(schema.parse_ts(received) - _td(BOT_WINDOW_S))
        burst = conn.execute(
            "SELECT event_id, minute, type FROM events WHERE tenant_id=? AND anonymous_id=? "
            "AND type='page_view' AND is_bot=0 AND received_at>=? AND received_at<=?",
            (tenant, anon, window_start, received)).fetchall()
        if len(burst) >= BOT_BURST:
            conn.execute("INSERT OR IGNORE INTO bots VALUES(?,?,?,?)",
                         (tenant, anon, f"{len(burst)} page views in {BOT_WINDOW_S}s", burst[0]["event_id"]))
            # Correct the aggregates retroactively: these were counted as humans.
            for row in conn.execute(
                    "SELECT event_id, minute, type FROM events WHERE tenant_id=? AND anonymous_id=? AND is_bot=0",
                    (tenant, anon)).fetchall():
                if row["type"] in ROLLUP_TYPES:
                    _bump(conn, tenant, row["minute"], row["type"], -1)
            conn.execute("UPDATE events SET is_bot=1 WHERE tenant_id=? AND anonymous_id=?", (tenant, anon))
            _outcome(conn, part, seq, tenant, eid, "stored", ";".join(flags + ["bot_burst_detected"]), src)
            return

    _outcome(conn, part, seq, tenant, eid, "stored", ";".join(flags), src)


def erased_hash(tenant, value):
    import hashlib
    return hashlib.sha256(f"{tenant}|{value}".encode()).hexdigest()


def _td(seconds):
    from datetime import timedelta
    return timedelta(seconds=seconds)


def process_batch(conn, part, records):
    commit_ts = time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        for seq, rec in records:
            handle(conn, part, seq, rec, commit_ts)
        conn.execute("INSERT INTO offsets VALUES(?,?) ON CONFLICT(partition) DO UPDATE SET seq=excluded.seq",
                     (part, records[-1][0]))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def run(data_dir, n_partitions=4, batch=2000, idle_exit=False, idle_sleep=0.02, stop_file=None):
    conn = store.connect(os.path.join(data_dir, "store.db"))
    names = partitions(n_partitions)
    offsets = load_offsets(conn, names)
    log_dir = os.path.join(data_dir, "log")
    tailers = {n: Tailer(log_dir, n, offsets[n]) for n in names}
    total = 0
    while True:
        did = 0
        for n in names:
            recs = list(tailers[n].poll(limit=batch))
            if recs:
                process_batch(conn, n, recs)
                did += len(recs)
        total += did
        if not did:
            if idle_exit:
                break
            if stop_file and os.path.exists(stop_file):
                break
            time.sleep(idle_sleep)
    conn.close()
    return total
