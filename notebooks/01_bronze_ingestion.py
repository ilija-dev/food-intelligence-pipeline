# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC
# MAGIC **Purpose**: Load raw Open Food Facts data into a Delta Lake table with ZERO transformations.
# MAGIC
# MAGIC Bronze is the "insurance policy" layer. We preserve the full raw dataset — all 200+ columns,
# MAGIC nested JSON structures, duplicates, nulls, and garbage — exactly as it arrived from the source.
# MAGIC The only additions are metadata columns: when was this ingested, from what file, and a batch ID.
# MAGIC
# MAGIC **Why this matters**: If Silver logic has a bug 3 months from now, you replay from Bronze
# MAGIC instead of re-downloading 7GB from the internet and hoping the source hasn't changed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Get data from data sources" — ingesting external data into a lakehouse  
# MAGIC **DP-700 Mapping**: "Ingest and transform data" — implementing a Bronze layer with Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration

# COMMAND ----------

import yaml
import os
from datetime import datetime, timezone

# Load pipeline config
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config",
    "pipeline_config.yaml",
)

try:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
except (FileNotFoundError, NameError):
    WORKSPACE_CONFIG = "/Workspace/Repos/food-intelligence-pipeline/config/pipeline_config.yaml"
    with open(WORKSPACE_CONFIG, "r") as f:
        config = yaml.safe_load(f)

# Extract what we need
active_source = config["active_source"]
source_config = config["data_sources"][active_source]
db_name = config["database"]["name"]
bronze_path = config["delta_tables"]["bronze"]["products"]

print(f"Source: {active_source} ({source_config['format']})")
print(f"Source path: {source_config['dbfs_path']}")
print(f"Bronze Delta path: {bronze_path}")
print(f"Database: {db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Raw Data
# MAGIC
# MAGIC We read the raw data exactly as it is — no column selection, no casting, no filtering.
# MAGIC
# MAGIC - **Parquet**: `spark.read.parquet()` — schema is embedded in the file
# MAGIC - **JSONL**: `spark.read.json()` — Spark auto-infers the nested schema by scanning the file
# MAGIC
# MAGIC For JSONL, the auto-inference is important: the `nutriments` field alone has 100+ sub-fields
# MAGIC as a nested struct. Spark handles this automatically.

# COMMAND ----------

spark.sql(f"USE {db_name}")

source_path = source_config["dbfs_path"]
source_format = source_config["format"]

if source_format == "parquet":
    df_raw = spark.read.parquet(source_path)
elif source_format == "jsonl":
    # For JSONL: Spark scans the file to infer the full nested schema
    # This handles the 100+ nested nutriments fields automatically
    df_raw = spark.read.json(source_path)
else:
    raise ValueError(f"Unsupported format: {source_format}")

raw_count = df_raw.count()
raw_columns = len(df_raw.columns)

print(f"Raw data loaded: {raw_count:,} rows x {raw_columns} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add Metadata Columns
# MAGIC
# MAGIC These are the ONLY additions Bronze makes to the raw data. They answer three questions:
# MAGIC
# MAGIC - **`_ingestion_timestamp`**: When did this data arrive in our pipeline?
# MAGIC - **`_source_file`**: Where did it come from? (enables lineage tracking)
# MAGIC - **`_batch_id`**: Which ingestion run produced this? (enables idempotent re-runs)
# MAGIC
# MAGIC The underscore prefix (`_`) is a convention that marks these as pipeline metadata,
# MAGIC not original source data. Any engineer looking at the schema instantly knows these
# MAGIC were added by us.

# COMMAND ----------

from pyspark.sql import functions as F

# Generate a batch ID from the current timestamp
batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

df_bronze = df_raw.withColumns(
    {
        "_ingestion_timestamp": F.current_timestamp(),
        "_source_file": F.lit(source_path),
        "_source_format": F.lit(source_format),
        "_batch_id": F.lit(batch_id),
    }
)

print(f"Added 4 metadata columns. Total columns: {len(df_bronze.columns)}")
print(f"Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Lake
# MAGIC
# MAGIC We write as Delta format with `overwrite` mode. Key decisions:
# MAGIC
# MAGIC - **`mode("overwrite")`**: This is a full-refresh ingestion. Each run replaces the Bronze table
# MAGIC   entirely. For incremental ingestion (new products only), you'd use `merge` — but Open Food
# MAGIC   Facts provides a full dump, not a change feed.
# MAGIC
# MAGIC - **`option("overwriteSchema", "true")`**: Open Food Facts adds new fields over time.
# MAGIC   Schema evolution means we accept the new schema instead of failing. This is a real-world
# MAGIC   pattern — external sources change their schemas without warning.
# MAGIC
# MAGIC - **No partitioning in Bronze**: We don't know how the data will be queried yet.
# MAGIC   Partitioning is a Silver/Gold decision based on access patterns.
# MAGIC
# MAGIC - **Delta Lake instead of raw Parquet**: Gives us ACID transactions (write doesn't corrupt
# MAGIC   on failure), time travel (compare with previous ingestion), and schema enforcement.

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)

print(f"Bronze table written to: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register in Metastore
# MAGIC
# MAGIC Writing Delta files to DBFS creates the physical data. Registering as a table in the
# MAGIC metastore makes it queryable via SQL (`SELECT * FROM food_intelligence.bronze_products`).
# MAGIC This is the difference between "files on disk" and "a table in a database."

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.bronze_products
    USING DELTA
    LOCATION '{bronze_path}'
""")

print(f"Table registered: {db_name}.bronze_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation
# MAGIC
# MAGIC Post-write checks — never trust that a write succeeded without verifying.
# MAGIC We read the Delta table back and compare row counts.

# COMMAND ----------

# Read back from Delta and validate
df_verify = spark.read.format("delta").load(bronze_path)
bronze_count = df_verify.count()
bronze_columns = len(df_verify.columns)

# Validate row count matches
assert bronze_count == raw_count, (
    f"Row count mismatch! Raw: {raw_count:,}, Bronze: {bronze_count:,}"
)

print("=" * 60)
print("BRONZE INGESTION VALIDATION")
print("=" * 60)
print(f"Source rows:      {raw_count:,}")
print(f"Bronze rows:      {bronze_count:,}")
print(f"Match:            {'YES' if bronze_count == raw_count else 'NO'}")
print(f"Source columns:   {raw_columns}")
print(f"Bronze columns:   {bronze_columns} (+4 metadata)")
print(f"Batch ID:         {batch_id}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Schema Overview
# MAGIC
# MAGIC Print the full schema so we can see exactly what we're preserving.
# MAGIC For JSONL ingestion, this shows the nested `nutriments` struct with 100+ sub-fields.

# COMMAND ----------

# Print schema — for JSONL this reveals the nested complexity
print(f"Bronze table schema ({bronze_columns} columns):\n")

# Group columns for readability
metadata_cols = [c for c in df_verify.columns if c.startswith("_")]
data_cols = [c for c in df_verify.columns if not c.startswith("_")]

print(f"--- Pipeline metadata ({len(metadata_cols)} columns) ---")
for col in sorted(metadata_cols):
    dtype = str(df_verify.schema[col].dataType)
    print(f"  {col:<40} {dtype}")

print(f"\n--- Source data ({len(data_cols)} columns) ---")
for col in sorted(data_cols)[:50]:
    dtype = str(df_verify.schema[col].dataType)
    print(f"  {col:<40} {dtype}")

if len(data_cols) > 50:
    print(f"  ... and {len(data_cols) - 50} more columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Quick Data Profile
# MAGIC
# MAGIC A few quick stats to understand what we're working with before Silver.
# MAGIC This is NOT transformation — it's reconnaissance.

# COMMAND ----------

# Check key fields that Silver will care about
print("Key field null rates (what Silver will need to handle):\n")

key_fields = [
    "code", "product_name", "brands", "categories_tags",
    "countries_tags", "nutriscore_grade", "nova_group",
]

# Only profile fields that exist in this dataset
existing_fields = [f for f in key_fields if f in df_verify.columns]

for field in existing_fields:
    null_count = df_verify.filter(F.col(field).isNull()).count()
    null_pct = (null_count / bronze_count) * 100
    print(f"  {field:<30} {null_pct:>6.1f}% null  ({null_count:>10,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Delta Lake Features Demo
# MAGIC
# MAGIC Show that this is Delta, not just Parquet — we have transaction history.

# COMMAND ----------

# Show Delta table history — this proves ACID transactions are working
display(spark.sql(f"DESCRIBE HISTORY '{bronze_path}' LIMIT 5"))

# COMMAND ----------

# Show table details
display(spark.sql(f"DESCRIBE DETAIL '{bronze_path}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Save Ingestion Metadata
# MAGIC
# MAGIC Update the pipeline metadata file so downstream notebooks know when Bronze was last refreshed.

# COMMAND ----------

import json

metadata = {
    "bronze_row_count": bronze_count,
    "bronze_column_count": bronze_columns,
    "source": active_source,
    "source_format": source_format,
    "source_path": source_path,
    "batch_id": batch_id,
    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    "delta_path": bronze_path,
    "table_name": f"{db_name}.bronze_products",
}

metadata_path = "/FileStore/food-intelligence/metadata/bronze_metadata.json"
dbutils.fs.put(metadata_path, json.dumps(metadata, indent=2), overwrite=True)

print(f"Bronze metadata saved to: {metadata_path}")
print(json.dumps(metadata, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Item | Value |
# MAGIC |------|-------|
# MAGIC | Source | Open Food Facts ({active_source}) |
# MAGIC | Raw rows | {raw_count:,} |
# MAGIC | Bronze rows | {bronze_count:,} |
# MAGIC | Columns preserved | {raw_columns} source + 4 metadata |
# MAGIC | Transformations | ZERO (metadata columns only) |
# MAGIC | Format | Delta Lake |
# MAGIC | Table | `food_intelligence.bronze_products` |
# MAGIC
# MAGIC **Next step**: Run `02_silver_cleaning.py` to deduplicate, flatten, type-cast, and standardize.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "Bronze preserves the full 200+ column nested JSON structure from Open Food Facts — including
# MAGIC > the `nutriments` struct with 100+ sub-fields, multilingual product names, and duplicate barcodes
# MAGIC > from crowd-sourced contributions. I add only four metadata columns for lineage tracking. No
# MAGIC > transformations happen here. This means if my Silver dedup logic has a bug, or if I need a column
# MAGIC > I initially excluded, I replay from Bronze without re-downloading the 7GB source dump. I write as
# MAGIC > Delta with schema evolution enabled because Open Food Facts adds new fields without warning —
# MAGIC > `overwriteSchema` handles that gracefully instead of failing the pipeline."
