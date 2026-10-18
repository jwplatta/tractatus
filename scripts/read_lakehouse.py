"""One-off script to verify data is readable from the Iceberg lakehouse."""

from __future__ import annotations

import os

from pyiceberg.catalog import load_catalog

# Load config from env / .env
bucket = os.environ["ICEBERG_BUCKET"]
region = os.environ.get("AWS_REGION", "us-east-1")
profile = os.environ.get("AWS_PROFILE")
database = os.environ.get("GLUE_DATABASE", "tractatus")

props: dict[str, str] = {
    "type": "glue",
    "glue.region": region,
    "s3.region": region,
    "warehouse": f"s3://{bucket}",
}
if profile:
    props["glue.profile-name"] = profile
    props["s3.profile-name"] = profile

catalog = load_catalog("glue", **props)

# Load table
table = catalog.load_table(f"{database}.options")
print(f"Table: {table.name()}")
print(f"Schema: {table.schema()}")
print(f"Partition spec: {table.spec()}")
print(f"Snapshots: {len(table.history())}")
print()

# Scan and read into pandas
scan = table.scan()
df = scan.to_pandas()
print(f"Rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
print()

if len(df) > 0:
    print("Sample (first 5 rows, key columns):")
    cols = ["symbol", "contract_type", "strike", "expiration_date", "bid", "ask", "sampled_at"]
    available = [c for c in cols if c in df.columns]
    print(df[available].head().to_string(index=False))
    print()
    print(f"Unique sampled_at values: {sorted(df['sampled_at'].unique())}")
