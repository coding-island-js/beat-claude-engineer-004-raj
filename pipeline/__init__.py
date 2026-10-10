"""Real-time analytics pipeline prototype for Beat Claude engineer-004 (brief version 2026-07).

SDK event intake -> durable partitioned log -> processor (dedupe, normalize,
identity, bot and PII handling) -> real-time aggregates -> query API,
plus a reconciler that accounts for every input row.
Standard library only.
"""
