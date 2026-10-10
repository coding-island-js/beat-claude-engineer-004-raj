"""Monthly AWS cost of the proposed design vs the $50K/month ceiling.

  python cost/cost_model.py         # prints the table, writes cost/cost_report.md

Every price comes from cost/aws_prices.json (AWS Price List API snapshot,
SKU and publication date kept; refresh with cost/fetch_prices.py).
Every other input is labeled Observed / Estimated / Benchmarked / Assumed.
"""
import json
import math
import os

HERE = os.path.dirname(os.path.abspath(__file__))
P = {k: v["usd"] for k, v in json.load(open(os.path.join(HERE, "aws_prices.json")))["prices"].items()}
HOURS = 730  # AWS pricing month

# ---- inputs (label, value, why) -------------------------------------------
I = {
    "events_per_day": ("Brief", 50_000_000, "brief: ~50M events/day"),
    "spike_multiplier": ("Brief", 10, "brief: 10x traffic spikes"),
    "fixture_mean_bytes": ("Observed", 229, "mean line size of fixtures/event_sample.jsonl (25 rows)"),
    "event_bytes": ("Assumed", 1024, "real events carry user agent, full URLs, custom props; 4x the fixture"),
    "compression": ("Assumed", 5, "gzip on S3 and ClickHouse column compression on JSON-like events"),
    "retention_months": ("Assumed", 13, "13 months raw + hot, so year-over-year dashboards work"),
    "kinesis_headroom": ("Assumed", 2, "2x over the 10x peak for uneven partition keys"),
    "collector_req_per_task": ("Assumed", 1000, "per 1 vCPU Node task; local Python prototype did 3,000 req/s on one process (Observed, results/)"),
    "collector_tasks_avg": ("Estimated", 8, "autoscale 4..12; peak 5,787 req/s / 1,000 = 6, +2 for AZ spread"),
    "lambda_batch_avg": ("Estimated", 50, "~48 records/shard/s at average load with a 1s batching window"),
    "lambda_seconds": ("Assumed", 0.2, "per batch, includes Redis and ClickHouse round trips"),
    "lambda_gb": ("Assumed", 1.0, "memory size"),
    "alb_lcu_avg": ("Estimated", 24, "worst case one new TLS connection per beacon: ~600 new conn/s / 25 per LCU"),
    "query_tasks": ("Assumed", 4, "dashboard + personalization decision API, 1 vCPU / 2 GB each"),
    "log_gb": ("Assumed", 100, "batch-level logs only; per-event logging would be ~1,430 GB"),
    "bigquery_share": ("Assumed", 0.2, "share of export volume going cross-cloud to BigQuery"),
    "staging_share": ("Assumed", 0.25, "staging env as a share of compute"),
}
v = {k: x[1] for k, x in I.items()}

eps_avg = v["events_per_day"] / 86400
eps_peak = eps_avg * v["spike_multiplier"]
gb_month = v["events_per_day"] * 30 * v["event_bytes"] / 1024 ** 3
records_month = v["events_per_day"] * 30
stored_gb = gb_month / v["compression"] * v["retention_months"]

shards_by_records = math.ceil(eps_peak / 1000)
shards_by_bytes = math.ceil(eps_peak * v["event_bytes"] / 1024 ** 2)
shards_by_reads = math.ceil(eps_peak * v["event_bytes"] * 2 / (2 * 1024 ** 2))  # 2 consumers, 2 MB/s read each
shards = max(shards_by_records, shards_by_bytes, shards_by_reads) * v["kinesis_headroom"]

fargate_task = P["fargate_arm_vcpu_hour"] * 1 + P["fargate_arm_gb_hour"] * 2
lines = []


def add(component, usd, math_text, label):
    lines.append((component, usd, math_text, label))


add("Collector (ECS Fargate ARM, behind existing ALB)", v["collector_tasks_avg"] * fargate_task * HOURS,
    f"{v['collector_tasks_avg']} tasks x ${fargate_task:.4f}/h x {HOURS}h", "Estimated from AWS price")
add("ALB capacity units (incremental)", v["alb_lcu_avg"] * P["alb_lcu_hour"] * HOURS + P["alb_hour"] * HOURS,
    f"{v['alb_lcu_avg']} LCU x ${P['alb_lcu_hour']} x {HOURS}h + 1 ALB-hour", "Estimated from AWS price")
add("Kinesis Data Streams, provisioned", shards * P["kinesis_shard_hour"] * HOURS,
    f"{shards} shards (peak {eps_peak:,.0f} rec/s / 1,000 per shard = {shards_by_records}, x{v['kinesis_headroom']}) x ${P['kinesis_shard_hour']}/h", "Estimated from AWS price + AWS shard limits")
add("Kinesis 7-day retention (replay window for rollback)", shards * P["kinesis_extended_retention_shard_hour"] * HOURS,
    f"{shards} x ${P['kinesis_extended_retention_shard_hour']}/h", "Estimated from AWS price")
add("Kinesis PUT payload units", records_month * P["kinesis_put_payload_unit"],
    f"{records_month/1e9:.1f}B records x $0.014/M", "Estimated from AWS price")
inv = records_month / v["lambda_batch_avg"]
add("Lambda processor (ARM)", inv * P["lambda_request"] + inv * v["lambda_seconds"] * v["lambda_gb"] * P["lambda_gb_second_arm"],
    f"{inv/1e6:.0f}M invocations, {v['lambda_seconds']}s x {v['lambda_gb']}GB", "Estimated from AWS price")
fh_billed = records_month * 5 / 1024 ** 2  # 5 KB rounding per record, in GB
add("Firehose to S3 raw archive", fh_billed * P["firehose_kds_source_gb"],
    f"billed in 5 KB steps per record: {fh_billed:,.0f} GB x ${P['firehose_kds_source_gb']}", "Estimated from AWS price")
add("S3 raw archive (steady state)", stored_gb * P["s3_standard_gb_month"],
    f"{stored_gb:,.0f} GB ({gb_month:,.0f} GB/mo / {v['compression']} x {v['retention_months']} mo)", "Estimated from AWS price")
add("ClickHouse: 2 x r7g.2xlarge (replicated)", 2 * P["ec2_r7g_2xlarge_hour"] * HOURS,
    f"2 x ${P['ec2_r7g_2xlarge_hour']}/h", "Estimated from AWS price")
add("ClickHouse Keeper: 3 x t4g.small", 3 * P["ec2_t4g_small_hour"] * HOURS, f"3 x ${P['ec2_t4g_small_hour']}/h", "Estimated from AWS price")
add("ClickHouse gp3 storage (2 replicas, 30% free)", 2 * stored_gb * 1.3 * P["ebs_gp3_gb_month"],
    f"2 x {stored_gb*1.3:,.0f} GB x ${P['ebs_gp3_gb_month']}", "Estimated from AWS price")
add("ClickHouse backups to S3", stored_gb * P["s3_standard_gb_month"], f"{stored_gb:,.0f} GB", "Estimated from AWS price")
add("ElastiCache Redis 2 x cache.r7g.large", 2 * P["elasticache_r7g_large_hour"] * HOURS,
    f"primary + replica x ${P['elasticache_r7g_large_hour']}/h", "Estimated from AWS price")
add("Query + decision API (Fargate ARM)", v["query_tasks"] * fargate_task * HOURS, f"{v['query_tasks']} tasks", "Estimated from AWS price")
add("VPC interface endpoint for Kinesis (no NAT)", 3 * P["vpc_endpoint_hour"] * HOURS + gb_month * P["vpc_endpoint_gb"],
    "3 AZ x $0.01/h + GB processed", "Estimated from AWS price")
add("Inter-AZ traffic", (gb_month / 2 + gb_month / v["compression"]) * 2 * P["data_transfer_inter_az_gb"],
    "half of inserts cross AZ + CH replication, $0.01 each way", "Estimated from AWS price")
add("CloudWatch Logs", v["log_gb"] * P["cloudwatch_logs_ingest_gb"], f"{v['log_gb']} GB x $0.50", "Estimated from AWS price")
add("Warehouse export egress (BigQuery)", gb_month / v["compression"] * v["bigquery_share"] * P["data_transfer_out_internet_gb"],
    "Parquet to GCP; Snowflake on AWS reads S3 in-region", "Estimated from AWS price")
compute = sum(u for c, u, _, _ in lines if any(w in c for w in ("Collector", "Lambda", "ClickHouse: 2", "Redis", "Query")))
add("Staging environment", compute * v["staging_share"], f"{int(v['staging_share']*100)}% of compute", "Assumed share")

total = sum(u for _, u, _, _ in lines)
ondemand_alt = (P["kinesis_ondemand_stream_hour"] * HOURS + gb_month * P["kinesis_ondemand_gb_in"]
                + 2 * gb_month * P["kinesis_ondemand_gb_out"])
per_event_logging = gb_month * P["cloudwatch_logs_ingest_gb"]


def main():
    out = ["# Cost model (generated by `python cost/cost_model.py`)", "",
           "Prices: AWS Price List API, us-east-1, snapshot in `cost/aws_prices.json` (SKU + publication date per line).", "",
           "## Inputs", "", "| Input | Value | Label | Why |", "|---|---|---|---|"]
    for k, (label, val, why) in I.items():
        out.append(f"| {k} | {val:,} | {label} | {why} |" if isinstance(val, int) else f"| {k} | {val} | {label} | {why} |")
    out += ["", f"Derived: average {eps_avg:,.0f} ev/s, 10x peak {eps_peak:,.0f} ev/s, {gb_month:,.0f} GB/month ingested, "
            f"{stored_gb:,.0f} GB retained (all Estimated from the inputs above).", "",
            "## Monthly cost", "", "| Component | USD/month | Math | Label |", "|---|---:|---|---|"]
    for c, u, m, l in lines:
        out.append(f"| {c} | {u:,.0f} | {m} | {l} |")
    out += [f"| **Total** | **{total:,.0f}** | | Estimated |",
            f"| Ceiling | 50,000 | brief | |",
            f"| Share of ceiling | {total/50000:.1%} | | Estimated |", "",
            "## Alternatives priced and rejected", "",
            f"- Kinesis on-demand instead of provisioned: about ${ondemand_alt:,.0f}/month (Estimated). Cheaper to run, but AWS documents that on-demand "
            "only absorbs up to double the previous 30-day peak and can throttle when traffic more than doubles within 15 minutes. A 10x Black Friday spike "
            "breaks that rule, so provisioned shards sized for 10x win.",
            f"- Logging every event to CloudWatch would add about ${per_event_logging:,.0f}/month (Estimated). Batch-level logs only.",
            "- Firehose bills Kinesis-sourced records in 5 KB steps, so a 1 KB event is billed as 5 KB. Still small here; noted so nobody is surprised.",
            "- ClickHouse Cloud (managed) instead of self-managed EC2: not priced here (no public AWS price list). It trades money for team time, "
            "which is the scarcer resource. A procurement decision, see SUBMISSION.md."]
    open(os.path.join(HERE, "cost_report.md"), "w").write("\n".join(out) + "\n")
    for c, u, m, l in lines:
        print(f"{u:>9,.0f}  {c}")
    print(f"{total:>9,.0f}  TOTAL ({total/50000:.1%} of $50,000)")
    print(f"kinesis on-demand alt ${ondemand_alt:,.0f}; per-event logging ${per_event_logging:,.0f}; shards {shards}")


if __name__ == "__main__":
    main()
