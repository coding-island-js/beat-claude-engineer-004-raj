"""python -m unittest discover -s tests -v"""
import json
import os
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from pipeline import log, processor, store, schema, query, reconcile  # noqa: E402
from pipeline.log import DurableLog, partition_for  # noqa: E402


def ingest(data, events, n=4):
    lg = DurableLog(os.path.join(data, "log"), n)
    items = []
    for e in events:
        ok, rec = schema.intake_check(json.dumps(e).encode(), schema.now_utc())
        items.append((str(partition_for(rec.get("tenant_id"), rec.get("anonymous_id"), n)), rec))
    lg.append_many(items)
    lg.close()


def ev(eid, tenant="t-1", anon="a-1", etype="page_view", path="/", user=None):
    return {"event_id": eid, "tenant_id": tenant, "anonymous_id": anon, "user_id": user, "type": etype,
            "ts": schema.fmt_ts(schema.now_utc()), "properties": {"path": path}}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def test_fixture_catches_every_class_with_rows(self):
        import run_fixture
        self.assertEqual(run_fixture.main(), 0)
        rep = json.load(open(os.path.join(ROOT, "results", "fixture_report.json")))
        a = rep["anomalies"]
        ids = lambda k: sorted(r.get("event_id") or r.get("event_id_guess") for r in a[k]["rows"])
        self.assertEqual(ids("duplicate_retry"), ["evt-0002"])
        self.assertEqual(ids("malformed_json"), ["evt-0020"])
        self.assertEqual(ids("missing_tenant"), ["evt-0011"])
        self.assertEqual(ids("legacy_sdk_schema"), ["evt-0009"])
        self.assertEqual(ids("client_clock_ahead"), ["evt-0005", "evt-0006"])
        self.assertEqual(ids("ts_implausible_future"), ["evt-0016"])
        self.assertEqual(ids("pii_in_properties"), ["evt-0007"])
        self.assertEqual(ids("bot_burst"), ["evt-0012", "evt-0013", "evt-0014", "evt-0015"])
        self.assertEqual(ids("privacy_request_via_sdk"), ["evt-0017"])
        self.assertEqual(ids("client_computed_count"), ["evt-0019"])
        self.assertEqual(a["client_computed_count"]["rows"][0]["server_counted_pricing_views"], 1)
        self.assertTrue(rep["all_checks_passed"])
        # raw PII must not exist anywhere on disk after intake
        for root, _, files in os.walk(os.path.join(ROOT, ".data", "fixture")):
            for f in files:
                self.assertNotIn(b"maria.gonzalez", open(os.path.join(root, f), "rb").read(), f)

    def test_torn_write_is_truncated_on_recovery(self):
        ingest(self.d, [ev("e1"), ev("e2")], n=1)
        path = os.path.join(self.d, "log", "part-0.log")
        with open(path, "ab") as f:
            f.write(b"3\t12345\t{\"half\":")  # crash mid-write
        lg = DurableLog(os.path.join(self.d, "log"), 1)
        self.assertEqual(lg.next_seq["0"], 3)
        lg.close()
        self.assertTrue(open(path, "rb").read().endswith(b"\n"))

    def test_crash_mid_batch_rolls_back_and_never_double_counts(self):
        ingest(self.d, [ev(f"e{i}", anon=f"a-{i}") for i in range(10)], n=1)
        real = processor.handle
        calls = {"n": 0}

        def flaky(conn, part, seq, rec, ts):
            calls["n"] += 1
            if calls["n"] == 6:
                raise RuntimeError("simulated crash")
            return real(conn, part, seq, rec, ts)
        processor.handle = flaky
        try:
            with self.assertRaises(RuntimeError):
                processor.run(self.d, n_partitions=1, idle_exit=True)
        finally:
            processor.handle = real
        processor.run(self.d, n_partitions=1, idle_exit=True)
        conn = store.connect(os.path.join(self.d, "store.db"), readonly=True)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 10)
        self.assertEqual(conn.execute("SELECT SUM(n) FROM rollup_minute").fetchone()[0], 10)
        conn.close()
        self.assertTrue(reconcile.reconcile(self.d, n_partitions=1)["ok"])

    def test_same_id_different_body_is_quarantined_not_merged(self):
        a = ev("dup", path="/a")
        b = ev("dup", path="/b")
        ingest(self.d, [a, b], n=1)
        processor.run(self.d, n_partitions=1, idle_exit=True)
        conn = store.connect(os.path.join(self.d, "store.db"), readonly=True)
        reasons = [r[0] for r in conn.execute("SELECT reason FROM quarantine")]
        self.assertEqual(reasons, ["event_id_collision"])
        conn.close()

    def test_tenant_isolation_in_identity_and_queries(self):
        ingest(self.d, [ev("x1", tenant="t-A", anon="shared", user="u-1", etype="identify"),
                        ev("x2", tenant="t-B", anon="shared", path="/pricing")])
        processor.run(self.d, idle_exit=True)
        conn = store.connect(os.path.join(self.d, "store.db"), readonly=True)
        v = query.visitor(conn, "t-B", "shared")
        self.assertEqual(v["ids"], ["shared"])          # u-1 belongs to t-A only
        self.assertEqual([e["event_id"] for e in v["events"]], ["x2"])
        self.assertEqual(query.visitor(conn, "t-B", "u-1")["events"], [])
        conn.close()


if __name__ == "__main__":
    unittest.main()
