"""Data-accuracy reconciler.

"Accuracy verified" has a testable definition here:
  1. Every request the client saw acknowledged (2xx/400) is in the log.   (no loss at intake)
  2. Every log record up to the committed offset has exactly one outcome. (no silent drop)
  3. stored events in the table == stored outcomes - deleted.            (no phantom rows)
  4. Rollup totals == human analytics events in the table.               (no double count)
  5. Unexplained difference = 0. Any other number fails the check.

The same report is how the parallel run is judged during migration: the
old system's count minus the new system's count must be fully explained
by named classes (duplicates, bots, quarantine, privacy), per tenant.
"""
import json
import os
from collections import Counter, defaultdict

from . import store
from .log import read_partition
from .processor import partitions, ROLLUP_TYPES


def reconcile(data_dir, n_partitions=4, client_ledger=None):
    conn = store.connect(os.path.join(data_dir, "store.db"), readonly=True)
    names = partitions(n_partitions)
    log_dir = os.path.join(data_dir, "log")
    report = {"checks": {}, "outcomes": {}, "per_tenant": {}}
    failures = []

    log_seqs = {n: [s for s, _ in read_partition(log_dir, n)] for n in names}
    offsets = {r["partition"]: r["seq"] for r in conn.execute("SELECT partition, seq FROM offsets")}
    outcome_rows = conn.execute("SELECT partition, seq, tenant_id, outcome FROM outcomes").fetchall()
    have = defaultdict(set)
    for r in outcome_rows:
        have[r["partition"]].add(r["seq"])

    # 1. client acknowledgements vs log
    if client_ledger:
        acked = set()
        for line in open(client_ledger, encoding="utf-8"):
            row = json.loads(line)
            for ref in row.get("seqs", []):
                acked.add(tuple(ref.split(":")))
        in_log = {(n, str(s)) for n in names for s in log_seqs[n]}
        deleted_refs = set()
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='compacted'").fetchone():
            deleted_refs = {(r["partition"], str(r["seq"])) for r in
                            conn.execute("SELECT partition, seq FROM compacted")}
        report["checks"]["acked_removed_by_gdpr_compaction"] = len(acked & deleted_refs)
        missing = sorted(acked - in_log - deleted_refs)
        report["checks"]["acked_not_in_log"] = len(missing)
        report["checks"]["acked_total"] = len(acked)
        if missing:
            failures.append(f"{len(missing)} acknowledged records missing from log: {missing[:5]}")

    # 2. every committed log record has exactly one outcome
    unexplained = 0
    for n in names:
        committed = offsets.get(n, 0)
        expected = {s for s in log_seqs[n] if s <= committed}
        miss = expected - have[n]
        unexplained += len(miss)
        if miss:
            failures.append(f"partition {n}: {len(miss)} records consumed with no outcome")
    report["checks"]["log_records"] = sum(len(v) for v in log_seqs.values())
    report["checks"]["records_with_outcome"] = len(outcome_rows)
    report["checks"]["uncommitted_tail"] = sum(len([s for s in log_seqs[n] if s > offsets.get(n, 0)]) for n in names)

    oc = Counter(r["outcome"] for r in outcome_rows)
    report["outcomes"] = dict(oc)

    # 3. table rows vs stored outcomes
    n_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    report["checks"]["event_rows"] = n_events
    if n_events != oc.get("stored", 0):
        failures.append(f"event rows {n_events} != stored outcomes {oc.get('stored', 0)}")

    # 4. rollups vs human analytics events
    types = tuple(ROLLUP_TYPES)
    marks = ",".join("?" * len(types))
    human = conn.execute(f"SELECT COUNT(*) FROM events WHERE is_bot=0 AND type IN ({marks})", types).fetchone()[0]
    rolled = conn.execute("SELECT COALESCE(SUM(n),0) FROM rollup_minute").fetchone()[0]
    report["checks"]["human_events"] = human
    report["checks"]["rollup_total"] = rolled
    if human != rolled:
        failures.append(f"rollup total {rolled} != human events {human} (double count or drop)")

    # per-tenant explanation: what a naive counter would report vs what we serve
    for r in conn.execute("SELECT tenant_id, outcome, COUNT(*) c FROM outcomes GROUP BY tenant_id, outcome"):
        report["per_tenant"].setdefault(r["tenant_id"] or "(no tenant)", {})[r["outcome"]] = r["c"]
    for r in conn.execute("SELECT tenant_id, SUM(is_bot) bots, COUNT(*) rows FROM events GROUP BY tenant_id"):
        t = report["per_tenant"].setdefault(r["tenant_id"], {})
        t["bot_rows"] = r["bots"]
        t["served_human_rows"] = r["rows"] - r["bots"]

    report["checks"]["unexplained_difference"] = unexplained
    report["ok"] = not failures
    report["failures"] = failures
    conn.close()
    return report
