# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Pipeline Setup
# MAGIC
# MAGIC **Purpose**: Initialize the food intelligence pipeline environment.
# MAGIC This notebook:
# MAGIC 1. Loads pipeline configuration from YAML
# MAGIC 2. Creates the database/schema in the metastore
# MAGIC 3. Downloads the Open Food Facts dataset to DBFS
# MAGIC 4. Validates that the data is accessible and prints basic stats
# MAGIC
# MAGIC **Run this once** before executing any other notebook.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Get data from analytical data stores" — configuring data sources  
# MAGIC **DP-700 Mapping**: "Ingest and transform data" — staging raw data in a lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration
# MAGIC
# MAGIC All paths, thresholds, and table names come from `pipeline_config.yaml`.  
# MAGIC No hardcoded values in notebooks — this is a production pattern.

# COMMAND ----------

import yaml
import os

# Load config — handles both local execution and Databricks notebooks
try:
    CONFIG_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "pipeline_config.yaml",
    )
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
except (NameError, FileNotFoundError):
    # Databricks notebook: __file__ is not defined
    # Update this path to match your Repos location: /Workspace/Repos/<username>/food-intelligence-pipeline/...
    WORKSPACE_CONFIG = "/Workspace/Users/ileristovski1@gmail.com/food-intelligence-pipeline/config/pipeline_config.yaml"
    with open(WORKSPACE_CONFIG, "r") as f:
        config = yaml.safe_load(f)

print("Configuration loaded successfully.")
print(f"Pipeline: {config['pipeline']['name']} v{config['pipeline']['version']}")
print(f"Active source: {config['active_source']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Database
# MAGIC
# MAGIC We create a dedicated database so all Bronze/Silver/Gold tables live under one namespace.
# MAGIC This is the Databricks equivalent of creating a schema in a traditional warehouse.

# COMMAND ----------

db_name = config["database"]["name"]

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")

print(f"Database '{db_name}' is ready.")
print(f"All tables will be created under: {db_name}.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create DBFS Directory Structure
# MAGIC
# MAGIC DBFS (Databricks File System) is the underlying storage.
# MAGIC We create separate directories for raw data and Delta tables per layer.

# COMMAND ----------

# Create directory structure in DBFS
directories = [
    "/FileStore/food-intelligence/raw",
    config["delta_tables"]["bronze"]["products"],
    config["delta_tables"]["silver"]["products"],
]

# Add all Gold table paths
for table_name, path in config["delta_tables"]["gold"].items():
    directories.append(path)

for d in directories:
    dbutils.fs.mkdirs(d)
    print(f"  Created: {d}")

print("\nDBFS directory structure is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Download Dataset
# MAGIC
# MAGIC We download directly from the source into DBFS.
# MAGIC
# MAGIC **Parquet option** (~1.2GB): Fastest start, pre-filtered columns from HuggingFace.  
# MAGIC **JSONL option** (~7GB compressed): Full dataset with 200+ columns — more impressive for interviews.
# MAGIC
# MAGIC The active source is controlled by `active_source` in the config.

# COMMAND ----------

active = config["active_source"]
source_config = config["data_sources"][active]

print(f"Active source: {active}")
print(f"URL: {source_config['url']}")
print(f"DBFS destination: {source_config['dbfs_path']}")

# COMMAND ----------

# Check if file already exists to avoid re-downloading
file_exists = False
try:
    file_info = dbutils.fs.ls(source_config["dbfs_path"])
    file_exists = True
    print(f"File already exists at {source_config['dbfs_path']}")
    print(f"Size: {file_info[0].size / (1024**3):.2f} GB")
except Exception:
    file_exists = False
    print("File not found. Will download.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download using `%sh` (wget)
# MAGIC
# MAGIC Databricks `%sh` runs on the driver node's local filesystem.
# MAGIC We download there first, then move to DBFS.
# MAGIC
# MAGIC **Note**: On Databricks Community Edition, the driver has limited disk.
# MAGIC The Parquet file (~1.2GB) fits comfortably. The full JSONL (~7GB compressed)
# MAGIC may require you to process it in a streaming fashion instead.

# COMMAND ----------

if not file_exists:
    import subprocess
    import shutil

    url = source_config["url"]
    local_filename = url.split("/")[-1]
    local_path = f"/tmp/{local_filename}"

    print(f"Downloading {local_filename}...")
    print(f"This may take a few minutes depending on your connection speed.\n")

    # Download with wget — shows progress
    result = subprocess.run(
        ["wget", "-q", "--show-progress", "-O", local_path, url],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        file_size = os.path.getsize(local_path)
        print(f"\nDownload complete: {file_size / (1024**3):.2f} GB")

        # Move from local filesystem to DBFS
        dbfs_dest = source_config["dbfs_path"]
        dbutils.fs.cp(f"file:{local_path}", dbfs_dest)
        print(f"Moved to DBFS: {dbfs_dest}")

        # Clean up local temp file
        os.remove(local_path)
        print("Cleaned up temp file.")
    else:
        print(f"Download failed: {result.stderr}")
        print("You can manually upload the file to DBFS using the Databricks UI:")
        print(f"  1. Download from: {url}")
        print(f"  2. Upload to: {source_config['dbfs_path']}")
else:
    print("Skipping download — file already exists.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate Data Access
# MAGIC
# MAGIC Quick sanity check: can Spark read the file? How many rows? What does the schema look like?
# MAGIC This catches issues early — before you're deep into the Bronze notebook wondering why things fail.

# COMMAND ----------

# Read the raw data to validate access
source_format = source_config["format"]
source_path = source_config["dbfs_path"]

if source_format == "parquet":
    df_raw = spark.read.parquet(source_path)
elif source_format == "jsonl":
    # For JSONL, Spark auto-infers the nested schema
    df_raw = spark.read.json(source_path)
else:
    raise ValueError(f"Unsupported format: {source_format}")

row_count = df_raw.count()
col_count = len(df_raw.columns)

print("=" * 60)
print("DATA VALIDATION")
print("=" * 60)
print(f"Format:        {source_format}")
print(f"Location:      {source_path}")
print(f"Row count:     {row_count:,}")
print(f"Column count:  {col_count}")
print("=" * 60)

# COMMAND ----------

# Show schema overview — first 30 columns
print("Schema preview (first 30 columns):")
print("-" * 40)
for field in df_raw.schema.fields[:30]:
    print(f"  {field.name:<40} {str(field.dataType)}")

if col_count > 30:
    print(f"  ... and {col_count - 30} more columns")

# COMMAND ----------

# Preview a few rows
display(df_raw.select("code", "product_name", "brands", "categories_tags", "countries_tags").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save Metadata
# MAGIC
# MAGIC Store ingestion metadata so downstream notebooks can verify freshness.

# COMMAND ----------

from datetime import datetime, timezone

metadata = {
    "source": active,
    "source_url": source_config["url"],
    "source_path": source_config["dbfs_path"],
    "row_count": row_count,
    "column_count": col_count,
    "setup_timestamp": datetime.now(timezone.utc).isoformat(),
    "pipeline_version": config["pipeline"]["version"],
}

# Save as a small JSON file in DBFS for other notebooks to reference
import json

metadata_path = "/FileStore/food-intelligence/metadata/setup_metadata.json"
dbutils.fs.mkdirs("/FileStore/food-intelligence/metadata")
dbutils.fs.put(metadata_path, json.dumps(metadata, indent=2), overwrite=True)

print("Setup metadata saved to:", metadata_path)
print(json.dumps(metadata, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Item | Status |
# MAGIC |------|--------|
# MAGIC | Configuration | Loaded from YAML |
# MAGIC | Database | `food_intelligence` created |
# MAGIC | DBFS directories | Created for raw, bronze, silver, gold |
# MAGIC | Data download | Complete |
# MAGIC | Validation | Row count and schema verified |
# MAGIC | Metadata | Saved for downstream notebooks |
# MAGIC
# MAGIC **Next step**: Run `01_bronze_ingestion.py` to load raw data into a Delta Lake table.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "Every pipeline I build starts with a configuration-driven setup notebook. Paths, thresholds,
# MAGIC > and quality rules live in YAML — not scattered across notebooks. This means switching from a
# MAGIC > 1GB Parquet dev dataset to the full 43GB JSONL production dataset is a one-line config change,
# MAGIC > not a refactor. On the Databricks Free Edition, I used DBFS as the storage layer, which maps
# MAGIC > directly to ADLS in a production Azure Databricks deployment."
