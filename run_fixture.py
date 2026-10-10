"""One command: python run_fixture.py

Replays the challenge fixture through the real pipeline over HTTP
(intake -> durable log -> processor -> store -> query API), then:
  1. prints the anomaly report, class by class, with event_id and fixture line
  2. replays the whole fixture a second time to prove it is idempotent
  3. executes the verified GDPR deletion for the evt-0017 subject and checks
     nothing is left
  4. replays the fixture a third time (a backfill from the raw archive) and
     checks the erased person does not come back
  5. runs the reconciler after every step; any unexplained row fails the run
Writes results/fixture_report.json and results/fixture_report.md.
Standard library only. Exit code 0 only if every check passes.
"""
import hashlib
import http.client as httpc
import json
import os
import shutil
import socket
import sys
import threading

from pipeline import intake, processor, query, reconcile, privacy, store

ROOT = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(ROOT, "fixtures", "event_sample.jsonl")
DATA = os.path.join(ROOT, ".data", "fixture")
RESULTS = os.path.join(ROOT, "results")
EXPECTED_SHA = "1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def call(method, port, path, body=None, headers=None):
    c = httpc.HTTPConnection("127.0.0.1", port, timeout=30)
    c.request(method, path, body=body, headers=headers or {})
    r = c.getresponse()
    data = r.read()
    c.close()
    return r.status, json.loads(data) if data else None


def replay(port, lines, ledger_path, label):
    with open(ledger_path, "a", encoding="utf-8") as ledger:
        for i, raw in enumerate(lines, 1):
            status, body = call("POST", port, "/v1/events", raw,
                                {"Content-Type": "application/json", "X-Source-Ref": f"{label}:line{i}"})
            seqs = (body or {}).get("seqs") or (body or {}).get("quarantined") or []
            ledger.write(json.dumps({"ref": f"{label}:line{i}", "status": status, "seqs": seqs}) + "\n")


def anomaly_report(conn, q_port):
    rows = lambda sql, *a: [dict(r) for r in conn.execute(sql, a)]
    def line(src):
        return src.split(":")[1] if src else None
    rep = {}
    dups = rows("SELECT event_id, src, detail FROM outcomes WHERE outcome='duplicate' AND src LIKE 'pass1:%'")
    rep["duplicate_retry"] = {
        "rows": [{"event_id": d["event_id"], "line": line(d["src"]), "first_seen": d["detail"].split(" of ")[-1]} for d in dups],
        "action": "Dropped as a retry: same (tenant_id, event_id) and same content hash (received_at differs). Counted once."}
    mal = rows("SELECT event_id, src, reason FROM quarantine WHERE reason='malformed_json' AND src LIKE 'pass1:%'")
    rep["malformed_json"] = {
        "rows": [{"event_id_guess": m["event_id"], "line": line(m["src"])} for m in mal],
        "action": "Raw bytes kept in the quarantine partition (counted, never dropped), 400 to the client. Not auto-repaired: a truncated body may be missing fields."}
    nt = rows("SELECT event_id, src FROM quarantine WHERE reason='missing_tenant' AND src LIKE 'pass1:%'")
    rep["missing_tenant"] = {
        "rows": [{"event_id": m["event_id"], "line": line(m["src"])} for m in nt],
        "action": "Quarantined. Never guessed from the URL; one wrong guess is a cross-tenant leak. In production the tenant comes from the write key, not the payload."}
    flagged = rows("SELECT event_id, src, flags, event_time, received_at FROM events WHERE flags<>'' ORDER BY event_id")
    pick = lambda key: [{"event_id": f["event_id"], "line": line(f["src"]), "flags": f["flags"],
                         "event_time_used": f["event_time"]} for f in flagged if key in f["flags"]]
    rep["legacy_sdk_schema"] = {"rows": pick("legacy_schema"),
        "action": "Mapped pageview->page_view, timestamp->ts, page_path->path, ref->referrer. Accepted, because customers cannot be forced to update the SDK. No received_at: flagged, client time used."}
    rep["client_clock_ahead"] = {"rows": pick("client_clock_ahead"),
        "action": "Client ts is later than server receive time by more than 5s. Server received_at used as event time; original ts kept."}
    rep["ts_implausible_future"] = {"rows": pick("ts_implausible_future"),
        "action": "Client ts more than 24h ahead (here a year). Server received_at used; never allowed to create a future dashboard bucket."}
    rep["pii_in_properties"] = {"rows": pick("pii_scrubbed"),
        "action": "Email and phone replaced with a keyed hash at intake, before the durable write. Raw PII never reaches the log, the store or exports."}
    bots = rows("SELECT anonymous_id, reason, first_event_id FROM bots")
    bot_rows = rows("SELECT event_id, src, anonymous_id FROM events WHERE is_bot=1 ORDER BY event_id")
    rep["bot_burst"] = {"visitors": bots,
        "rows": [{"event_id": b["event_id"], "line": line(b["src"])} for b in bot_rows],
        "action": "4 page views in under 1s from one visitor. Kept in raw, excluded from dashboards and segments, earlier counts corrected. The scanner referrer is only a hint, never the rule."}
    pr = rows("SELECT request_event_id, status, regulation, src FROM privacy_requests")
    rep["privacy_request_via_sdk"] = {"rows": [{"event_id": p["request_event_id"], "line": line(p["src"]),
                                                "status_after_ingest": "pending_verification"} for p in pr],
        "action": "Not executed from the stream: the SDK write key is public, so anyone could send it. Queued for the tenant to verify; executed later through the authenticated path (step 3)."}
    cc = pick("client_count_untrusted")
    for c in cc:
        ev = conn.execute("SELECT user_id, anonymous_id, tenant_id, props FROM events WHERE event_id=?",
                          (c["event_id"],)).fetchone()
        _, v = call("GET", q_port, f"/v1/tenants/{ev['tenant_id']}/visitors/{ev['user_id'] or ev['anonymous_id']}")
        c["client_claimed"] = json.loads(ev["props"]).get("count_today")
        c["server_counted_pricing_views"] = v["server_pricing_views"]
        c["server_pricing_event_ids"] = v["pricing_view_event_ids"]
    rep["client_computed_count"] = {"rows": cc,
        "action": "Client-computed count is not trusted. The SDK says 3 today; this 17-minute sample holds 1 server-seen pricing view. The sample cannot say which is right, so segments are built only from server-counted events."}
    ident = rows("SELECT anonymous_id, user_id, source, event_id FROM identity ORDER BY event_id")
    rep["identity_stitching"] = {"links": ident,
        "action": "anon->user links per tenant, from any event that carries both ids, except unverified privacy requests. Stitched at read time through the link table, not by rewriting history, so a deletion removes one link instead of rewriting every event."}
    return rep


def main():
    sha = hashlib.sha256(open(FIXTURE, "rb").read()).hexdigest()
    print(f"fixture: fixtures/event_sample.jsonl  sha256={sha}")
    if sha != EXPECTED_SHA:
        print("WARNING: fixture checksum differs from the one this submission was built on")
    lines = [l for l in open(FIXTURE, "rb").read().split(b"\n") if l.strip()]
    print(f"rows in fixture: {len(lines)}")

    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA)
    os.makedirs(RESULTS, exist_ok=True)
    ledger = os.path.join(DATA, "client_ledger.jsonl")

    i_port, q_port = free_port(), free_port()
    isrv = intake.build(DATA, i_port, preserve_received_at=True)
    qsrv = query.build(DATA, q_port)
    for s in (isrv, qsrv):
        threading.Thread(target=s.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()

    out = {"fixture_sha256": sha, "rows": len(lines), "steps": {}}
    ok = True

    # 1. ingest + process
    replay(i_port, lines, ledger, "pass1")
    processor.run(DATA, idle_exit=True)
    conn = store.connect(os.path.join(DATA, "store.db"), readonly=True)
    out["anomalies"] = anomaly_report(conn, q_port)
    out["steps"]["1_ingest"] = reconcile.reconcile(DATA, client_ledger=ledger)
    _, out["live_t042"] = call("GET", q_port, "/v1/tenants/t-042/live")
    _, out["visitor_u7304"] = call("GET", q_port, "/v1/tenants/t-042/visitors/u-7304")
    _, out["segment_viewed_pricing_t042"] = call("GET", q_port, "/v1/tenants/t-042/segments/viewed_pricing?min=3")
    rollup_before = out["live_t042"]["total_human_events"]
    conn.close()

    # 2. idempotent replay of the same 25 rows
    replay(i_port, lines, ledger, "pass2")
    processor.run(DATA, idle_exit=True)
    r2 = reconcile.reconcile(DATA, client_ledger=ledger)
    _, live2 = call("GET", q_port, "/v1/tenants/t-042/live")
    r2["t042_human_events_before_after"] = [rollup_before, live2["total_human_events"]]
    r2["idempotent"] = rollup_before == live2["total_human_events"]
    out["steps"]["2_replay_same_data"] = r2
    ok &= r2["idempotent"]

    # 3. verified GDPR deletion for the evt-0017 subject (tenant t-088)
    isrv.shutdown(); isrv.server_close(); isrv.pipeline_log.close()   # compaction needs the log closed
    out["steps"]["3_gdpr_delete"] = privacy.delete_subject(DATA, "t-088", "u-1077", request_event_id="evt-0017")
    out["steps"]["3_gdpr_delete"]["reconcile"] = reconcile.reconcile(DATA, client_ledger=ledger)
    ok &= out["steps"]["3_gdpr_delete"]["ok"]

    # 4. backfill from the raw archive after the deletion
    i_port = free_port()
    isrv = intake.build(DATA, i_port, preserve_received_at=True)
    threading.Thread(target=isrv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
    replay(i_port, lines, ledger, "pass3")
    processor.run(DATA, idle_exit=True)
    conn = store.connect(os.path.join(DATA, "store.db"), readonly=True)
    resurrected = conn.execute("SELECT COUNT(*) FROM events WHERE tenant_id='t-088' AND "
                               "(anonymous_id='anon-77a' OR user_id='u-1077')").fetchone()[0]
    suppressed = [dict(r) for r in conn.execute("SELECT event_id, src FROM outcomes WHERE outcome='suppressed_erased'")]
    conn.close()
    r4 = reconcile.reconcile(DATA, client_ledger=ledger)
    r4["erased_subject_rows_after_backfill"] = resurrected
    r4["suppressed_on_backfill"] = suppressed
    out["steps"]["4_backfill_after_delete"] = r4
    ok &= resurrected == 0

    for k, v in out["steps"].items():
        rec = v.get("reconcile", v)
        ok &= rec["ok"]
    out["all_checks_passed"] = bool(ok)
    isrv.shutdown(); qsrv.shutdown()

    with open(os.path.join(RESULTS, "fixture_report.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1)
    write_markdown(out)
    print_summary(out)
    return 0 if ok else 1


def write_markdown(out):
    a = out["anomalies"]
    L = ["# Fixture report (generated by `python run_fixture.py`)", "",
         f"Fixture sha256: `{out['fixture_sha256']}` ({out['rows']} rows)", "",
         "| Class | Rows (event_id @ fixture line) | What the pipeline did |", "|---|---|---|"]
    def cell(rows, key="event_id"):
        return ", ".join(f"{r.get(key) or r.get('event_id_guess')} @{r.get('line')}" for r in rows)
    for k, v in a.items():
        if k == "identity_stitching":
            cellv = ", ".join(f"{l['anonymous_id']}->{l['user_id']} ({l['source']} {l['event_id']})" for l in v["links"])
        else:
            cellv = cell(v.get("rows", []))
        L.append(f"| {k} | {cellv} | {v['action']} |")
    L += ["", "## Reconciler after each step", "", "| Step | Log records | With outcome | Unexplained | Outcomes | OK |", "|---|---|---|---|---|---|"]
    for k, v in out["steps"].items():
        r = v.get("reconcile", v)
        L.append(f"| {k} | {r['checks']['log_records']} | {r['checks']['records_with_outcome']} | "
                 f"{r['checks']['unexplained_difference']} | {r['outcomes']} | {r['ok']} |")
    d = out["steps"]["3_gdpr_delete"]
    L += ["", f"GDPR delete: subject ids {d['subject_ids']}, event rows deleted {d['deleted_event_ids']}, "
          f"raw log records removed {d['raw_log_records_removed']}, left after delete {d['remaining_after_delete']}.",
          f"Backfill after delete: erased subject rows = {out['steps']['4_backfill_after_delete']['erased_subject_rows_after_backfill']}.",
          f"Replay idempotent (t-042 human events before/after): {out['steps']['2_replay_same_data']['t042_human_events_before_after']}.",
          "", f"All checks passed: **{out['all_checks_passed']}**"]
    with open(os.path.join(RESULTS, "fixture_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")


def print_summary(out):
    print()
    for k, v in out["anomalies"].items():
        if k == "identity_stitching":
            print(f"  {k:24s} " + ", ".join(f"{l['anonymous_id']}->{l['user_id']}" for l in v["links"]))
            continue
        ids = [r.get("event_id") or r.get("event_id_guess") for r in v.get("rows", [])]
        print(f"  {k:24s} {', '.join(ids)}")
    print()
    for k, v in out["steps"].items():
        r = v.get("reconcile", v)
        print(f"  reconcile {k:26s} ok={r['ok']} unexplained={r['checks']['unexplained_difference']} outcomes={r['outcomes']}")
    print(f"\n  replay idempotent: {out['steps']['2_replay_same_data']['idempotent']}")
    d = out["steps"]["3_gdpr_delete"]
    print(f"  gdpr delete ok: {d['ok']} deleted={d['deleted_event_ids']} left={d['remaining_after_delete']}")
    print(f"  erased rows after backfill: {out['steps']['4_backfill_after_delete']['erased_subject_rows_after_backfill']}")
    print(f"\nALL CHECKS PASSED: {out['all_checks_passed']}   (details: results/fixture_report.md)")


if __name__ == "__main__":
    sys.exit(main())
