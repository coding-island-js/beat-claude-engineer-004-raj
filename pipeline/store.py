"""Serving store (SQLite in WAL mode).

Stands in for two AWS pieces: Redis (per-visitor state for personalization)
and ClickHouse (per-tenant rollups and event rows for dashboards).
The property that matters is the same in both: the event rows, the
aggregates, the outcome ledger and the consumer offset are written in ONE
transaction, so a crash can never count an event without also recording
that it was consumed, or the other way round.
"""
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS offsets(
  partition TEXT PRIMARY KEY, seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS outcomes(
  partition TEXT, seq INTEGER, tenant_id TEXT, event_id TEXT,
  outcome TEXT, detail TEXT, src TEXT,
  PRIMARY KEY(partition, seq));
CREATE TABLE IF NOT EXISTS events(
  tenant_id TEXT, event_id TEXT, content_hash TEXT,
  anonymous_id TEXT, user_id TEXT, type TEXT,
  event_time TEXT, received_at TEXT, minute TEXT, path TEXT,
  props TEXT, flags TEXT, is_bot INTEGER DEFAULT 0,
  ingest_ts REAL, committed_ts REAL, src TEXT,
  PRIMARY KEY(tenant_id, event_id));
CREATE INDEX IF NOT EXISTS ev_visitor ON events(tenant_id, anonymous_id, received_at);
CREATE INDEX IF NOT EXISTS ev_user ON events(tenant_id, user_id);
CREATE TABLE IF NOT EXISTS rollup_minute(
  tenant_id TEXT, minute TEXT, type TEXT, n INTEGER,
  PRIMARY KEY(tenant_id, minute, type));
CREATE TABLE IF NOT EXISTS identity(
  tenant_id TEXT, anonymous_id TEXT, user_id TEXT, source TEXT, event_id TEXT,
  PRIMARY KEY(tenant_id, anonymous_id, user_id));
CREATE TABLE IF NOT EXISTS bots(
  tenant_id TEXT, anonymous_id TEXT, reason TEXT, first_event_id TEXT,
  PRIMARY KEY(tenant_id, anonymous_id));
CREATE TABLE IF NOT EXISTS quarantine(
  partition TEXT, seq INTEGER, tenant_id TEXT, event_id TEXT,
  reason TEXT, raw TEXT, src TEXT,
  PRIMARY KEY(partition, seq));
CREATE TABLE IF NOT EXISTS privacy_requests(
  tenant_id TEXT, request_event_id TEXT, user_id TEXT, anonymous_id TEXT,
  regulation TEXT, status TEXT, received_at TEXT, executed_at TEXT, src TEXT,
  PRIMARY KEY(tenant_id, request_event_id));
CREATE TABLE IF NOT EXISTS erased(
  tenant_id TEXT, id_hash TEXT, erased_at TEXT,
  PRIMARY KEY(tenant_id, id_hash));
CREATE TABLE IF NOT EXISTS compacted(
  partition TEXT, seq INTEGER, reason TEXT, PRIMARY KEY(partition, seq));
CREATE TABLE IF NOT EXISTS audit(
  ts TEXT, action TEXT, detail TEXT);
"""


def connect(path, readonly=False):
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30, check_same_thread=False)
    else:
        conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript(SCHEMA)
    conn.row_factory = sqlite3.Row
    return conn
