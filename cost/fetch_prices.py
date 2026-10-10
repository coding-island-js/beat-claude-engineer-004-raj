"""Re-fetch every AWS price used in cost_model.py from the public AWS Price
List API (no login, no key) and write cost/aws_prices.json.

  python cost/fetch_prices.py            # ~20 MB of downloads, EC2 streamed
Region: us-east-1 (assumed; the brief does not say which region they run in).
"""
import csv
import io
import json
import os
import urllib.request

BASE = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{svc}/current/us-east-1/index.{ext}"
HERE = os.path.dirname(os.path.abspath(__file__))

# (key, service, SKU, OnDemand rate description must contain, beginRange)
WANTED = [
    ("kinesis_shard_hour", "AmazonKinesis", "7RXVY4V5SG8NQC38", None, "0"),
    ("kinesis_extended_retention_shard_hour", "AmazonKinesis", "9DT967X6YQX3AYMB", None, "0"),
    ("kinesis_put_payload_unit", "AmazonKinesis", "MAFXKQJ2YAQM6UDY", None, "0"),
    ("kinesis_ondemand_stream_hour", "AmazonKinesis", "S69RFKAH58MYJ6TF", None, "0"),
    ("kinesis_ondemand_gb_in", "AmazonKinesis", "826E7FJQWVKN3CBH", None, "0"),
    ("kinesis_ondemand_gb_out", "AmazonKinesis", "PNHYMNK9TM59MTU8", None, "0"),
    ("lambda_request", "AWSLambda", "K7BX6567RJ67A2KE", None, "0"),
    ("lambda_gb_second_arm", "AWSLambda", "72SBSFWPMDTH8S3J", None, "0"),
    ("firehose_kds_source_gb", "AmazonKinesisFirehose", "9HMQK22ZQCCJWFF6", None, "0"),
    ("firehose_kds_no_rounding_gb", "AmazonKinesisFirehose", "7FBBNJEEPEC6TFHX", None, "0"),
    ("fargate_arm_vcpu_hour", "AmazonECS", "XSZATS4VYMDC9CYN", None, "0"),
    ("fargate_arm_gb_hour", "AmazonECS", "UNH9KPQP7W7C66C9", None, "0"),
    ("s3_standard_gb_month", "AmazonS3", "WP9ANXZGBYYSGJEA", None, "0"),
    ("s3_put_request", "AmazonS3", "E9YHNFENF4XQBZR6", None, "0"),
    ("elasticache_r7g_large_hour", "AmazonElastiCache", "MWNGNHD8QDQRAGKN", None, "0"),
    ("data_transfer_out_internet_gb", "AWSDataTransfer", "HQEH3ZWJVT46JHRG", None, "0"),
    ("data_transfer_inter_az_gb", "AWSDataTransfer", "PNUBVW4CPC8XA46W", None, "0"),
    ("cloudwatch_logs_ingest_gb", "AmazonCloudWatch", "S8QGXX5R2BKKMDSJ", None, "0"),
    ("alb_hour", "AWSELB", "37CUWUT8GSNQEPUV", None, "0"),
    ("alb_lcu_hour", "AWSELB", "P2XGEJ8N3KU52WA8", None, "0"),
    ("vpc_endpoint_hour", "AmazonVPC", "EN2N5TATXE673A3B", None, "0"),
    ("vpc_endpoint_gb", "AmazonVPC", "DQYYEZBCWSW5XS2R", None, "0"),
]
EC2_WANTED = {  # key: (instance type or storage marker)
    "ec2_r7g_2xlarge_hour": "r7g.2xlarge",
    "ec2_t4g_small_hour": "t4g.small",
}


def main():
    out = {"region": "us-east-1", "source": "AWS Price List Bulk API (public, no login)", "prices": {}}
    by_svc = {}
    for key, svc, sku, _, begin in WANTED:
        if svc not in by_svc:
            url = BASE.format(svc=svc, ext="json")
            by_svc[svc] = (url, json.load(urllib.request.urlopen(url)))
        url, d = by_svc[svc]
        for term in d["terms"]["OnDemand"][sku].values():
            for dim in term["priceDimensions"].values():
                if dim.get("beginRange") == begin:
                    out["prices"][key] = {"usd": float(dim["pricePerUnit"]["USD"]), "unit": dim["unit"],
                                          "description": dim["description"], "sku": sku, "service": svc,
                                          "publication_date": d.get("publicationDate"), "url": url}
    # EC2 file is several GB as JSON; stream the CSV and keep only what we need
    url = BASE.format(svc="AmazonEC2", ext="csv")
    resp = io.TextIOWrapper(urllib.request.urlopen(url), encoding="utf-8")
    pub = None
    header = None
    for row in csv.reader(resp):
        if row and row[0] == "Publication Date":
            pub = row[1]
        if row and row[0] == "SKU":
            header = {h: i for i, h in enumerate(row)}
            continue
        if not header or len(row) < len(header):
            continue
        g = lambda k: row[header[k]]
        if g("TermType") != "OnDemand":
            continue
        if "gp3" in g("PriceDescription") and g("Unit") == "GB-Mo" and "ebs_gp3_gb_month" not in out["prices"]:
            out["prices"]["ebs_gp3_gb_month"] = {"usd": float(g("PricePerUnit")), "unit": "GB-Mo",
                                                 "description": g("PriceDescription"), "sku": g("SKU"),
                                                 "service": "AmazonEC2", "publication_date": pub, "url": url}
        for key, itype in EC2_WANTED.items():
            if (g("Instance Type") == itype and g("Operating System") == "Linux" and g("Tenancy") == "Shared"
                    and g("Pre Installed S/W") == "NA" and g("CapacityStatus") == "Used"
                    and g("operation") == "RunInstances"):
                out["prices"][key] = {"usd": float(g("PricePerUnit")), "unit": "Hrs",
                                      "description": g("PriceDescription"), "sku": g("SKU"),
                                      "service": "AmazonEC2", "publication_date": pub, "url": url}
        if len(out["prices"]) == len(WANTED) + len(EC2_WANTED) + 1:
            break
    with open(os.path.join(HERE, "aws_prices.json"), "w") as f:
        json.dump(out, f, indent=1)
    print(f"wrote {len(out['prices'])} prices to cost/aws_prices.json")


if __name__ == "__main__":
    main()
