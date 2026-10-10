"""python -m pipeline intake|process|query|reconcile|delete  --data DIR [--port N]"""
import argparse
import json
import sys

from . import intake, processor, query, reconcile, privacy


def main(argv=None):
    p = argparse.ArgumentParser(prog="pipeline")
    p.add_argument("cmd", choices=["intake", "process", "query", "reconcile", "delete"])
    p.add_argument("--data", required=True)
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--partitions", type=int, default=4)
    p.add_argument("--ledger")
    p.add_argument("--tenant")
    p.add_argument("--subject")
    p.add_argument("--stop-file")
    a = p.parse_args(argv)
    if a.cmd == "intake":
        intake.serve(a.data, a.port, a.partitions)
    elif a.cmd == "process":
        processor.run(a.data, a.partitions, stop_file=a.stop_file)
    elif a.cmd == "query":
        query.build(a.data, a.port, a.partitions).serve_forever()
    elif a.cmd == "reconcile":
        r = reconcile.reconcile(a.data, a.partitions, a.ledger)
        print(json.dumps(r, indent=1))
        return 0 if r["ok"] else 1
    elif a.cmd == "delete":
        r = privacy.delete_subject(a.data, a.tenant, a.subject, n_partitions=a.partitions)
        print(json.dumps(r, indent=1))
        return 0 if r["ok"] else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
