"""Load, spike and crash test against the local prototype.

  python loadtest.py                       # default: 300 ev/s base, 10x spike
  python loadtest.py --base 300 --spike 10 --batch 1

What it does (all real, on this machine, numbers written to results/):
  - starts intake, processor and query as separate OS processes
  - open-loop load: events are sent on a schedule, so a slow server cannot
    slow the generator down and hide its own latency (coordinated omission)
  - phases: base -> 10x spike -> base
  - 3% of sends are exact re-sends (SDK retries); 0.5% are malformed bodies
  - hard-kills the PROCESSOR mid-spike and restarts it
  - hard-kills the INTAKE later and restarts it; clients retry like the SDK
  - probes event->queryable latency through the query API all run long
  - at the end: reconciler + exact check that stored rows == distinct
    acknowledged events (no loss, no double count)

Honest scope: one Windows PC, Python standard library, single intake
process, SQLite. It proves the mechanics (durability, idempotency, crash
recovery, backpressure). It does NOT prove AWS throughput; that sizing
comes from AWS service limits, see SUBMISSION.md.
"""
import argparse
import http.client
import json
import multiprocessing as mp
import os
import platform
import random
import shutil
import socket
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.abspath(__file__))


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def pct(values, p):
    if not values:
        return None
    v = sorted(values)
    k = min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))
    return round(v[k], 2)


def start(cmd, data, port=None, extra=()):
    args = [sys.executable, "-m", "pipeline", cmd, "--data", data] + (["--port", str(port)] if port else []) + list(extra)
    return subprocess.Popen(args, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=open(os.path.join(data, f"{cmd}.err"), "a"))


def wait_port(port, timeout=15):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def make_event(wid, i, rng):
    tenant = f"t-{int(rng.paretovariate(1.2)) % 500:03d}"
    t = rng.random()
    etype = "page_view" if t < 0.6 else "click" if t < 0.85 else "form_submit" if t < 0.9 else "custom"
    return {"event_id": f"lt-{wid}-{i}", "tenant_id": tenant, "anonymous_id": f"anon-{rng.randrange(20000)}",
            "user_id": None, "type": etype, "ts": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "properties": {"path": rng.choice(["/", "/pricing", "/features", "/docs", "/blog/x"]),
                           "referrer": "https://google.com"}}


def worker(wid, port, schedule, batch, out_dir, t0, dup_rate, bad_rate):
    """schedule: list of (start_offset_s, end_offset_s, events_per_s for this worker)."""
    rng = random.Random(wid)
    conn = [None]
    ledger = open(os.path.join(out_dir, f"ledger-{wid}.jsonl"), "w")
    stats = {"sent_requests": 0, "events_sent": 0, "acked_events": 0, "status": {}, "retries": 0,
             "gave_up": 0, "lat_service_ms": [], "lat_sched_ms": [], "lat_phase": [],
             "valid_ids_acked": [], "dups_sent": 0, "bad_sent": 0}
    sent_bodies = []
    i = 0

    def post(body):
        for attempt in range(8):
            try:
                if conn[0] is None:
                    conn[0] = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                conn[0].request("POST", "/v1/events", body=body, headers={"Content-Type": "application/json"})
                r = conn[0].getresponse()
                data = r.read()
                if r.status == 503:
                    stats["retries"] += 1
                    time.sleep(0.05 * 2 ** attempt)
                    continue
                return r.status, json.loads(data) if data else {}
            except (OSError, http.client.HTTPException):
                try:
                    conn[0].close()
                except Exception:
                    pass
                conn[0] = None
                stats["retries"] += 1
                time.sleep(0.05 * 2 ** attempt)
        stats["gave_up"] += 1
        return None, None

    for (a, b, rate, phase) in schedule:
        if rate <= 0:
            continue
        interval = batch / rate
        n_req = int((b - a) * rate / batch)
        for k in range(n_req):
            sched = t0 + a + k * interval
            now = time.time()
            if sched > now:
                time.sleep(sched - now)
            events, valid_ids = [], []
            raw_override = None
            for _ in range(batch):
                roll = rng.random()
                if roll < bad_rate and batch == 1:  # a JSON batch cannot carry broken bytes
                    raw_override = b'{"event_id":"bad-' + str(i).encode() + b'","tenant_id":"t-001"'  # truncated
                    stats["bad_sent"] += 1
                    i += 1
                    continue
                if roll < bad_rate + dup_rate and sent_bodies:
                    e = rng.choice(sent_bodies)
                    stats["dups_sent"] += 1
                else:
                    e = make_event(wid, i, rng)
                    i += 1
                    sent_bodies.append(e)
                    if len(sent_bodies) > 5000:
                        sent_bodies.pop(0)
                events.append(e)
                valid_ids.append(e["event_id"])
            if raw_override is not None and not events:
                body = raw_override
            else:
                body = json.dumps(events[0] if batch == 1 else {"batch": events}).encode()
            t_send = time.time()
            status, resp = post(body)
            t_done = time.time()
            stats["sent_requests"] += 1
            stats["events_sent"] += max(1, len(events))
            stats["status"][str(status)] = stats["status"].get(str(status), 0) + 1
            stats["lat_service_ms"].append((t_done - t_send) * 1000)
            stats["lat_sched_ms"].append((t_done - sched) * 1000)
            stats["lat_phase"].append(phase)
            if status in (202, 400) and resp:
                seqs = resp.get("seqs") or resp.get("quarantined") or []
                ledger.write(json.dumps({"seqs": seqs}) + "\n")
                if status == 202:
                    stats["acked_events"] += resp.get("accepted", 0)
                    stats["valid_ids_acked"].extend(valid_ids)
    ledger.close()
    with open(os.path.join(out_dir, f"stats-{wid}.json"), "w") as f:
        json.dump(stats, f)


def probe_loop(i_port, q_port, stop, results):
    """Every 250 ms: send one event, poll the query API until it is visible."""
    n = 0
    ic = qc = None
    while not stop.is_set():
        n += 1
        eid = f"probe-{n}"
        body = json.dumps({"event_id": eid, "tenant_id": "t-probe", "anonymous_id": "anon-probe",
                           "type": "custom", "ts": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
                           "properties": {"name": "probe"}}).encode()
        t0 = time.time()
        try:
            ic = ic or http.client.HTTPConnection("127.0.0.1", i_port, timeout=10)
            ic.request("POST", "/v1/events", body=body, headers={"Content-Type": "application/json"})
            r = ic.getresponse(); r.read()
            if r.status != 202:
                raise OSError("not accepted")
            visible = None
            while time.time() - t0 < 30 and not stop.is_set():
                qc = qc or http.client.HTTPConnection("127.0.0.1", q_port, timeout=10)
                qc.request("GET", f"/v1/tenants/t-probe/events/{eid}")
                rr = qc.getresponse(); rr.read()
                if rr.status == 200:
                    visible = time.time()
                    break
                time.sleep(0.01)
            if visible:
                results.append({"t": round(t0 - results_t0[0], 2), "ms": round((visible - t0) * 1000, 1)})
        except (OSError, http.client.HTTPException):
            ic = qc = None
        time.sleep(0.25)


results_t0 = [0.0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=float, default=300, help="baseline events/s")
    ap.add_argument("--spike", type=float, default=10, help="spike multiplier")
    ap.add_argument("--batch", type=int, default=1, help="events per HTTP request")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--base-s", type=float, default=20)
    ap.add_argument("--spike-s", type=float, default=20)
    ap.add_argument("--recover-s", type=float, default=30)
    ap.add_argument("--no-crash", action="store_true")
    ap.add_argument("--tag", default="")
    a = ap.parse_args()

    data = os.path.join(ROOT, ".data", "loadtest")
    shutil.rmtree(data, ignore_errors=True)
    os.makedirs(data)
    out_dir = os.path.join(data, "client")
    os.makedirs(out_dir)
    i_port, q_port = free_port(), free_port()
    stop_file = os.path.join(data, "STOP")
    intake_p = start("intake", data, i_port)
    proc_p = start("process", data, extra=["--stop-file", stop_file])
    query_p = start("query", data, q_port)
    assert wait_port(i_port) and wait_port(q_port), "servers did not start"

    phases = [(0, a.base_s, a.base, "base"),
              (a.base_s, a.base_s + a.spike_s, a.base * a.spike, "spike"),
              (a.base_s + a.spike_s, a.base_s + a.spike_s + a.recover_s, a.base, "recover")]
    total_s = phases[-1][1]
    per_worker = [(s, e, r / a.workers, name) for s, e, r, name in phases]
    t0 = time.time() + 1.0
    results_t0[0] = t0
    procs = [mp.Process(target=worker, args=(w, i_port, per_worker, a.batch, out_dir, t0, 0.03, 0.005))
             for w in range(a.workers)]
    for p in procs:
        p.start()

    probes, stop = [], threading.Event()
    threading.Thread(target=probe_loop, args=(i_port, q_port, stop, probes), daemon=True).start()

    events_log, lag_samples = [], []
    crash_proc_at = a.base_s + a.spike_s * 0.5
    crash_intake_at = a.base_s + a.spike_s + a.recover_s * 0.4
    did_proc = did_intake = a.no_crash
    while any(p.is_alive() for p in procs):
        el = time.time() - t0
        if not did_proc and el >= crash_proc_at:
            proc_p.kill(); proc_p.wait()
            events_log.append({"t": round(el, 2), "abs": time.time(), "event": "processor hard-killed (TerminateProcess)"})
            time.sleep(3)
            proc_p = start("process", data, extra=["--stop-file", stop_file])
            events_log.append({"t": round(time.time() - t0, 2), "event": "processor restarted"})
            did_proc = True
        if not did_intake and el >= crash_intake_at:
            intake_p.kill(); intake_p.wait()
            events_log.append({"t": round(el, 2), "abs": time.time(), "event": "intake hard-killed (TerminateProcess)"})
            time.sleep(1)
            intake_p = start("intake", data, i_port)
            wait_port(i_port)
            events_log.append({"t": round(time.time() - t0, 2), "event": "intake restarted"})
            did_intake = True
        try:
            c = http.client.HTTPConnection("127.0.0.1", q_port, timeout=5)
            c.request("GET", "/v1/lag"); r = c.getresponse(); lag = json.loads(r.read())
            lag_samples.append({"t": round(el, 1), "lag": sum(v["lag"] for v in lag.values())})
        except Exception:
            pass
        time.sleep(1.0)
    for p in procs:
        p.join()
    t_end_load = time.time()
    # drain: wait for the processor to catch up
    drain_s = None
    while time.time() - t_end_load < 120:
        c = http.client.HTTPConnection("127.0.0.1", q_port, timeout=5)
        c.request("GET", "/v1/lag"); lag = json.loads(c.getresponse().read())
        if sum(v["lag"] for v in lag.values()) == 0:
            drain_s = round(time.time() - t_end_load, 2)
            break
        time.sleep(0.25)
    stop.set()
    time.sleep(0.5)
    open(stop_file, "w").close()
    proc_p.wait(timeout=60)
    intake_p.kill(); query_p.kill()

    # merge client stats
    agg = {"sent_requests": 0, "events_sent": 0, "acked_events": 0, "retries": 0, "gave_up": 0,
           "dups_sent": 0, "bad_sent": 0, "status": {}}
    lat = {"base": [], "spike": [], "recover": []}
    lat_sched = {"base": [], "spike": [], "recover": []}
    valid_ids = []
    with open(os.path.join(data, "client_ledger.jsonl"), "w") as merged:
        for w in range(a.workers):
            s = json.load(open(os.path.join(out_dir, f"stats-{w}.json")))
            for k in ("sent_requests", "events_sent", "acked_events", "retries", "gave_up", "dups_sent", "bad_sent"):
                agg[k] += s[k]
            for k, v in s["status"].items():
                agg["status"][k] = agg["status"].get(k, 0) + v
            for ms, msc, ph in zip(s["lat_service_ms"], s["lat_sched_ms"], s["lat_phase"]):
                lat[ph].append(ms); lat_sched[ph].append(msc)
            valid_ids.extend(s["valid_ids_acked"])
            merged.write(open(os.path.join(out_dir, f"ledger-{w}.jsonl")).read())

    sys.path.insert(0, ROOT)
    from pipeline import reconcile, store
    rec = reconcile.reconcile(data, client_ledger=os.path.join(data, "client_ledger.jsonl"))
    conn = store.connect(os.path.join(data, "store.db"), readonly=True)
    stored_lt = conn.execute("SELECT COUNT(*) FROM events WHERE event_id LIKE 'lt-%'").fetchone()[0]
    rows = conn.execute("SELECT ingest_ts, (committed_ts - ingest_ts)*1000 FROM events WHERE ingest_ts IS NOT NULL").fetchall()
    fresh = [r[1] for r in rows]
    # outside fault windows: from 1s before each kill to 10s after it
    kills = [e["abs"] for e in events_log if "abs" in e]
    fresh_nf = [r[1] for r in rows if not any(k - 1 <= r[0] <= k + 10 for k in kills)]
    conn.close()
    distinct_acked = len(set(valid_ids))

    probe_ms = [p["ms"] for p in probes]
    spike_window = (a.base_s, a.base_s + a.spike_s)
    probe_spike = [p["ms"] for p in probes if spike_window[0] <= p["t"] < spike_window[1]]
    result = {
        "tag": a.tag, "when_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine": {"os": platform.platform(), "cpu": platform.processor(), "logical_cpus": os.cpu_count(),
                    "python": platform.python_version()},
        "config": {"base_eps": a.base, "spike_mult": a.spike, "spike_eps": a.base * a.spike, "batch": a.batch,
                   "workers": a.workers, "phases_s": [a.base_s, a.spike_s, a.recover_s],
                   "dup_rate": 0.03, "malformed_rate": 0.005, "crash_tests": not a.no_crash},
        "client": agg,
        "achieved_eps_overall": round(agg["events_sent"] / total_s, 1),
        "accept_latency_ms": {ph: {"n": len(v), "p50": pct(v, 50), "p95": pct(v, 95), "p99": pct(v, 99), "max": pct(v, 100)}
                              for ph, v in lat.items()},
        "accept_latency_from_schedule_ms": {ph: {"p50": pct(v, 50), "p95": pct(v, 95), "p99": pct(v, 99), "max": pct(v, 100)}
                                            for ph, v in lat_sched.items()},
        "processor_freshness_ms": {"n": len(fresh), "p50": pct(fresh, 50), "p95": pct(fresh, 95), "p99": pct(fresh, 99), "max": pct(fresh, 100)},
        "processor_freshness_ms_outside_fault_windows": {"n": len(fresh_nf), "p50": pct(fresh_nf, 50), "p95": pct(fresh_nf, 95),
                                                          "p99": pct(fresh_nf, 99), "max": pct(fresh_nf, 100)},
        "event_to_queryable_probe_ms": {"n": len(probe_ms), "p50": pct(probe_ms, 50), "p95": pct(probe_ms, 95),
                                        "max": pct(probe_ms, 100), "spike_p95": pct(probe_spike, 95),
                                        "over_5s": sum(1 for x in probe_ms if x > 5000)},
        "max_consumer_lag_records": max((s["lag"] for s in lag_samples), default=0),
        "lag_timeline": lag_samples, "drain_after_load_s": drain_s,
        "fault_injection": events_log,
        "exactness": {"distinct_valid_events_acked": distinct_acked, "stored_rows": stored_lt,
                      "lost": distinct_acked - stored_lt if stored_lt <= distinct_acked else 0,
                      "double_counted": stored_lt - distinct_acked if stored_lt > distinct_acked else 0},
        "reconcile": {"ok": rec["ok"], "checks": rec["checks"], "outcomes": rec["outcomes"], "failures": rec["failures"]},
        "probe_timeline": probes,
    }
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    name = f"loadtest_{int(a.base)}x{int(a.spike)}_b{a.batch}{('_' + a.tag) if a.tag else ''}.json"
    with open(os.path.join(ROOT, "results", name), "w") as f:
        json.dump(result, f, indent=1)
    s = {k: v for k, v in result.items() if k not in ("lag_timeline", "probe_timeline")}
    print(json.dumps(s, indent=1))
    print(f"\nwritten: results/{name}")
    ok = rec["ok"] and result["exactness"]["lost"] == 0 and result["exactness"]["double_counted"] == 0 and agg["gave_up"] == 0
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
