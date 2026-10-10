# Real-time analytics pipeline prototype (Beat Claude engineer-004)

A working, runnable model of the pipeline in [SUBMISSION.md](SUBMISSION.md) ([PDF](SUBMISSION.pdf)).
Brief version 2026-07. Built against fixture sha256 `448b6db3c2708ff2a5244a596aa2c6ddb2bbf9644478c56a72e96c127e784bf2`.

Python 3.9 or newer. Standard library only. No install, no login, no keys, no network.

## Run it

```
python run_fixture.py
```

That one command replays the 25 fixture rows over HTTP through the whole pipeline, prints every anomaly with its `event_id`, and checks the counts. It exits 0 only if every check passes. Then:

```
python -m unittest discover -s tests          # 5 tests, under a second
python loadtest.py --base 300 --spike 10      # load + 10x spike + two hard kills, about 2 minutes
python cost/cost_model.py                     # monthly AWS cost from real AWS prices
python cost/fetch_prices.py                   # optional: re-pull those prices from the AWS Price List API
```

## What each piece is

| File | Stands in for (AWS design) | What it does |
|---|---|---|
| `pipeline/intake.py` | Collector on ECS Fargate | Accepts the unchanged SDK payload (and the old `pageview` shape). Scrubs PII. Answers 202 only after the record is fsynced. Bounded queue, 503 when full. |
| `pipeline/log.py` | Kinesis Data Streams | Append-only log, partitioned by tenant + visitor, CRC per record, group-commit fsync, torn-write recovery. |
| `pipeline/processor.py` | Lambda on the stream | Dedupe on (tenant, event_id) + content hash, legacy mapping, clock rules, identity links, bot bursts, privacy requests. Rows, rollups, outcome ledger and offset commit in one transaction. |
| `pipeline/query.py` | Dashboard + decision APIs | Per-tenant live counts, visitor profile with server-side counts, "viewed pricing 3x" segment, lag. |
| `pipeline/reconcile.py` | Reconciler | Every acknowledged request is in the log. Every log record has exactly one outcome. Rollups equal human events. Unexplained must be 0. |
| `pipeline/privacy.py` | Deletion job | Verified GDPR/CCPA delete across rows, links, rollups and the raw log, then re-checks that nothing is left. |
| `loadtest.py` | | Open-loop load with a 10x spike, retries, malformed bodies, hard kills of processor and intake. |
| `cost/` | | Cost model against the $50K ceiling. Prices with SKU and publication date. |

## Results already in the repo

- `results/fixture_report.md` and `.json`: each anomaly class with rows, and the reconciler after each step.
- `results/loadtest_*.json`: three load runs on one Windows PC (32 logical CPUs, Python 3.12). Zero lost, zero double counted in each.
- `cost/cost_report.md`: the cost table.

Honest scope: this is one machine, Python and SQLite. It proves the mechanics: durability, idempotency, crash recovery, deletion. It does not prove AWS throughput.

## Data

`fixtures/event_sample.jsonl` is the synthetic sample from the challenge repo, unchanged. The load test generates synthetic events. No real personal data anywhere. The PII salt in `pipeline/schema.py` is a labeled demo value.
