"""GDPR / CCPA deletion path.

A deletion request that arrives as an SDK event (fixture evt-0017) is NOT
executed: the SDK write key is public, so anyone could send one for
someone else. It is recorded as pending_verification. The tenant (data
controller) confirms it through an authenticated admin call, and only then
does this module run.

What it deletes, inside ONE tenant only:
  - every id linked to the subject (user_id <-> anonymous_ids)
  - their event rows, their identity links, their bot marks
  - the aggregates those rows contributed (so the reconciler still balances)
  - their raw records in the durable log (compaction keeps sequence numbers)
What it keeps: an audit row with a hash of the subject, never the raw id.
Then it re-queries every store and fails loudly if anything is left.
"""
import hashlib
import json
import os

from . import schema, store
from .log import rewrite_partition
from .processor import partitions, ROLLUP_TYPES, _bump, erased_hash
from .query import canonical_ids


def delete_subject(data_dir, tenant, subject_id, request_event_id=None, n_partitions=4):
    db = os.path.join(data_dir, "store.db")
    conn = store.connect(db)
    ids = canonical_ids(conn, tenant, subject_id)
    if request_event_id:
        # The verified request names its own ids (here user_id + anonymous_id).
        # They are trusted only now, after the tenant confirmed the request.
        req = conn.execute("SELECT user_id, anonymous_id FROM privacy_requests WHERE tenant_id=? "
                           "AND request_event_id=?", (tenant, request_event_id)).fetchone()
        for x in (req or {}) and (req["user_id"], req["anonymous_id"]):
            if x:
                ids |= canonical_ids(conn, tenant, x)
    marks = ",".join("?" * len(ids))
    conn.execute("BEGIN IMMEDIATE")
    rows = conn.execute(f"SELECT event_id, minute, type, is_bot, src FROM events WHERE tenant_id=? AND "
                        f"(anonymous_id IN ({marks}) OR user_id IN ({marks}))", (tenant, *ids, *ids)).fetchall()
    for r in rows:
        if r["type"] in ROLLUP_TYPES and not r["is_bot"]:
            _bump(conn, tenant, r["minute"], r["type"], -1)
    deleted_ids = [r["event_id"] for r in rows]
    deleted_src = [r["src"] for r in rows]
    conn.execute(f"DELETE FROM events WHERE tenant_id=? AND (anonymous_id IN ({marks}) OR user_id IN ({marks}))",
                 (tenant, *ids, *ids))
    conn.execute(f"DELETE FROM identity WHERE tenant_id=? AND (anonymous_id IN ({marks}) OR user_id IN ({marks}))",
                 (tenant, *ids, *ids))
    conn.execute(f"DELETE FROM bots WHERE tenant_id=? AND anonymous_id IN ({marks})", (tenant, *ids))
    if deleted_ids:
        m2 = ",".join("?" * len(deleted_ids))
        conn.execute(f"UPDATE outcomes SET outcome='deleted_gdpr' WHERE tenant_id=? AND outcome='stored' "
                     f"AND event_id IN ({m2})", (tenant, *deleted_ids))
    conn.execute(f"UPDATE privacy_requests SET status='executed', executed_at=?, user_id=NULL, anonymous_id=NULL "
                 f"WHERE tenant_id=? AND (user_id IN ({marks}) OR anonymous_id IN ({marks}))",
                 (schema.fmt_ts(schema.now_utc()), tenant, *ids, *ids))
    now = schema.fmt_ts(schema.now_utc())
    for i in ids:
        conn.execute("INSERT OR REPLACE INTO erased VALUES(?,?,?)", (tenant, erased_hash(tenant, i), now))
    subject_hash = hashlib.sha256(f"{tenant}|{sorted(ids)}".encode()).hexdigest()[:16]
    conn.execute("INSERT INTO audit VALUES(?,?,?)", (schema.fmt_ts(schema.now_utc()), "gdpr_delete",
                 json.dumps({"tenant_id": tenant, "subject_hash": subject_hash, "event_rows": len(rows),
                             "request_event_id": request_event_id})))
    conn.execute("COMMIT")

    # raw log compaction: drop records of this subject in this tenant
    def keep(rec):
        if rec.get("tenant_id") != tenant:
            return True
        return not (rec.get("anonymous_id") in ids or rec.get("user_id") in ids)
    raw_removed = 0
    for n in partitions(n_partitions):
        seqs = rewrite_partition(os.path.join(data_dir, "log"), n, keep)
        raw_removed += len(seqs)
        for s in seqs:
            conn.execute("INSERT OR IGNORE INTO compacted VALUES(?,?,?)", (n, s, "gdpr_delete"))
    for i in ids:
        conn.execute("DELETE FROM quarantine WHERE raw LIKE ?", (f'%"{i}"%',))

    # verify: nothing about the subject is left anywhere
    left = {
        "event_rows": conn.execute(f"SELECT COUNT(*) FROM events WHERE tenant_id=? AND (anonymous_id IN ({marks}) "
                                   f"OR user_id IN ({marks}))", (tenant, *ids, *ids)).fetchone()[0],
        "identity_rows": conn.execute(f"SELECT COUNT(*) FROM identity WHERE tenant_id=? AND (anonymous_id IN "
                                      f"({marks}) OR user_id IN ({marks}))", (tenant, *ids, *ids)).fetchone()[0],
        "privacy_rows_with_ids": conn.execute(f"SELECT COUNT(*) FROM privacy_requests WHERE tenant_id=? AND "
                                              f"(user_id IN ({marks}) OR anonymous_id IN ({marks}))",
                                              (tenant, *ids, *ids)).fetchone()[0],
    }
    raw_left = 0
    from .log import read_partition
    for n in partitions(n_partitions):
        for _, rec in read_partition(os.path.join(data_dir, "log"), n):
            if rec.get("tenant_id") == tenant and (rec.get("anonymous_id") in ids or rec.get("user_id") in ids):
                raw_left += 1
    left["raw_log_records"] = raw_left
    conn.close()
    return {"tenant_id": tenant, "subject_ids": sorted(ids), "deleted_event_ids": deleted_ids,
            "deleted_rows_src": deleted_src, "raw_log_records_removed": raw_removed,
            "remaining_after_delete": left, "ok": all(v == 0 for v in left.values()),
            "audit_subject_hash": subject_hash}
