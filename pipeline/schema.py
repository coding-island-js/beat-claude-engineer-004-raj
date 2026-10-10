"""Event contract: parse, normalize old SDK shapes, and classify problems.

Two places use this:
- intake (cheap checks only): parse JSON, find the tenant, scrub PII,
  stamp received_at. Runs before the durable write so raw PII never lands
  in the log.
- processor (full normalization): legacy field mapping, clock checks,
  untrusted client-side counts.

Every decision here is a named rule so the anomaly report can say which
rule fired on which event_id.
"""
import hashlib
import hmac
import json
import re
from datetime import datetime, timezone, timedelta

CANON_TYPES = {"page_view", "click", "form_submit", "custom", "identify", "privacy_request"}

# Older SDK builds still in the wild. We cannot force customers to update the
# SDK (brief constraint), so intake must accept these shapes forever.
LEGACY_TYPES = {"pageview": "page_view", "pageView": "page_view", "track": "custom"}
LEGACY_TOP_FIELDS = {"timestamp": "ts"}
LEGACY_PROP_FIELDS = {"page_path": "path", "ref": "referrer"}

# Clock rules (event time vs server receive time)
FUTURE_TOLERANCE = timedelta(seconds=5)    # small skew is normal network/clock jitter
IMPLAUSIBLE_FUTURE = timedelta(hours=24)   # beyond this the ts is not a clock drift, it is wrong
MAX_LATE = timedelta(days=7)               # offline SDK queues can flush late; older is clamped

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"^\+?\d[\d\-\s().]{7,}\d$")
PII_KEYS = {"email", "contact_email", "phone", "phone_number", "ssn", "full_name", "first_name", "last_name"}
# Not "name": on custom events it is the event name (evt-0016, evt-0019), not a person.

# Demo salt. In production: one secret per tenant in Secrets Manager.
PII_SALT = b"demo-only-salt-rotate-per-tenant"


def parse_ts(value):
    if not isinstance(value, str):
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_utc():
    return datetime.now(timezone.utc)


def pseudonymize(tenant_id, value):
    digest = hmac.new(PII_SALT + str(tenant_id).encode(), str(value).encode(), hashlib.sha256)
    return "pii:" + digest.hexdigest()[:16]


def scrub_pii(event):
    """Replace PII values in properties with a keyed hash. Returns list of keys scrubbed.
    The hash lets the tenant still join on it; the raw value never reaches the log."""
    props = event.get("properties")
    if not isinstance(props, dict):
        return []
    scrubbed = []
    for k, v in list(props.items()):
        if not isinstance(v, str):
            continue
        if k.lower() in PII_KEYS or EMAIL_RE.search(v) or (PHONE_RE.match(v) and "phone" in k.lower()):
            props[k] = pseudonymize(event.get("tenant_id"), v)
            scrubbed.append(k)
    return scrubbed


def intake_check(raw_bytes, server_now, preserve_received_at=False):
    """Run at the edge, before the durable write.
    Returns (ok, record_or_reason). Never raises."""
    try:
        event = json.loads(raw_bytes)
    except (ValueError, UnicodeDecodeError):
        return False, "malformed_json"
    if not isinstance(event, dict):
        return False, "malformed_json"
    flags = []
    if preserve_received_at:
        # Replay of a historical export: keep the original receive time.
        # A row without one never passed through a stamping intake (old path),
        # so the best available receive time is the client's own clock. Flagged.
        if not isinstance(event.get("received_at"), str):
            fallback = event.get("ts") or event.get("timestamp")
            event["received_at"] = fallback if parse_ts(fallback) else fmt_ts(server_now)
            flags.append("missing_received_at")
    else:
        event["received_at"] = fmt_ts(server_now)
    event["_ingest_ts"] = server_now.timestamp()  # server clock, used for freshness only
    scrubbed = scrub_pii(event)
    if scrubbed:
        flags.append("pii_scrubbed:" + ",".join(sorted(scrubbed)))
    event["_intake_flags"] = flags
    return True, event


def content_hash(event):
    """Identity of an event for dedupe: everything the client sent, minus
    fields that legitimately differ on a retry (receive time, ingest metadata)."""
    body = {k: v for k, v in event.items() if k not in ("received_at",) and not k.startswith("_")}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def normalize(event):
    """Full normalization in the processor.
    Returns (normalized_event | None, flags, quarantine_reason | None)."""
    flags = list(event.get("_intake_flags", []))
    e = dict(event)

    # 1. tenant: every row must belong to exactly one tenant
    if not e.get("tenant_id"):
        return None, flags, "missing_tenant"
    if not e.get("event_id"):
        return None, flags, "missing_event_id"

    # 2. legacy SDK shape -> current shape
    legacy = False
    for old, new in LEGACY_TOP_FIELDS.items():
        if old in e and new not in e:
            e[new] = e.pop(old)
            legacy = True
    if e.get("type") in LEGACY_TYPES:
        e["type"] = LEGACY_TYPES[e["type"]]
        legacy = True
    props = dict(e.get("properties") or {})
    for old, new in LEGACY_PROP_FIELDS.items():
        if old in props and new not in props:
            props[new] = props.pop(old)
            legacy = True
    e["properties"] = props
    if legacy:
        flags.append("legacy_schema")

    if e.get("type") not in CANON_TYPES:
        return None, flags, "unknown_type"

    # 3. clocks: client ts is a claim; server received_at is the fact
    received = parse_ts(e.get("received_at"))
    client_ts = parse_ts(e.get("ts"))
    if received is None:
        return None, flags, "no_receive_time"
    if client_ts is None:
        event_time = received
        flags.append("missing_client_ts")
    else:
        skew = client_ts - received
        if skew > IMPLAUSIBLE_FUTURE:
            flags.append(f"ts_implausible_future:{int(skew.total_seconds())}s")
            event_time = received
        elif skew > FUTURE_TOLERANCE:
            flags.append(f"client_clock_ahead:{skew.total_seconds():.1f}s")
            event_time = received
        elif -skew > MAX_LATE:
            flags.append("too_late_clamped")
            event_time = received
        else:
            event_time = client_ts
    e["event_time"] = fmt_ts(event_time)

    # 4. client-computed counters are not trusted (server recomputes them)
    for k in props:
        if k.startswith("count") or k.endswith("_count"):
            flags.append(f"client_count_untrusted:{k}")

    if e["type"] == "privacy_request":
        flags.append("privacy_request_unverified")

    return e, flags, None
