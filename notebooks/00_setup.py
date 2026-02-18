# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Pipeline Setup
# MAGIC
# MAGIC **Purpose**: Initialize the food intelligence pipeline environment.
# MAGIC This notebook:
# MAGIC 1. Loads pipeline configuration from YAML
# MAGIC 2. Creates the Unity Catalog schema
# MAGIC 3. Creates a volume for raw file storage
# MAGIC 4. Downloads the Open Food Facts dataset into the volume
# MAGIC 5. Validates that the data is accessible
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
    WORKSPACE_CONFIG = "/Workspace/Users/ileristovski1@gmail.com/food-intelligence-pipeline/config/pipeline_config.yaml"
    with open(WORKSPACE_CONFIG, "r") as f:
        config = yaml.safe_load(f)

print("Configuration loaded successfully.")
print(f"Pipeline: {config['pipeline']['name']} v{config['pipeline']['version']}")
print(f"Active source: {config['active_source']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Schema and Volume
# MAGIC
# MAGIC Unity Catalog hierarchy: **Catalog > Schema > Tables/Volumes**
# MAGIC - Schema = logical grouping (like a database)
# MAGIC - Volume = file storage (replaces DBFS)
# MAGIC - Managed tables = no explicit paths needed

# COMMAND ----------

catalog = config["catalog"]
schema = config["schema"]
volume = config["volume"]

# Full qualified names
full_schema = f"{catalog}.{schema}"
full_volume = f"{catalog}.{schema}.{volume}"
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
print(f"Schema ready: {full_schema}")

# Create volume for raw file storage
spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_volume}")
print(f"Volume ready: {full_volume}")
print(f"Volume path: {volume_path}")

# Set default schema so we don't need to qualify every table name
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"\nDefault context: {full_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Download Dataset
# MAGIC
# MAGIC We download directly into the Unity Catalog volume.
# MAGIC The volume is accessible at `/Volumes/<catalog>/<schema>/<volume>/`.

# COMMAND ----------

active = config["active_source"]
source_config = config["data_sources"][active]

source_url = source_config["url"]
local_filename = source_url.split("/")[-1]
volume_file_path = f"{volume_path}/{local_filename}"

print(f"Active source: {active}")
print(f"URL: {source_url}")
print(f"Volume destination: {volume_file_path}")

# COMMAND ----------

# Check if file already exists
file_exists = os.path.exists(volume_file_path)

if file_exists:
    file_size = os.path.getsize(volume_file_path)
    print(f"File already exists at {volume_file_path}")
    print(f"Size: {file_size / (1024**3):.2f} GB")
else:
    print("File not found. Will download.")

# COMMAND ----------

if not file_exists:
    import subprocess

    print(f"Downloading {local_filename}...")
    print(f"This may take a few minutes depending on connection speed.\n")

    # Download directly into the volume path
    result = subprocess.run(
        ["wget", "-q", "--show-progress", "-O", volume_file_path, source_url],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        file_size = os.path.getsize(volume_file_path)
        print(f"\nDownload complete: {file_size / (1024**3):.2f} GB")
        print(f"Saved to: {volume_file_path}")
    else:
        print(f"Download failed: {result.stderr}")
        print("\nAlternative: download manually and upload via Databricks UI")
        print(f"  1. Download from: {source_url}")
        print(f"  2. Upload to volume: {volume_path}/")
else:
    print("Skipping download — file already exists.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Data Access

# COMMAND ----------

source_format = source_config["format"]

if source_format == "parquet":
    df_raw = spark.read.parquet(volume_file_path)
elif source_format == "jsonl":
    df_raw = spark.read.json(volume_file_path)
else:
    raise ValueError(f"Unsupported format: {source_format}")

row_count = df_raw.count()
col_count = len(df_raw.columns)

print("=" * 60)
print("DATA VALIDATION")
print("=" * 60)
print(f"Format:        {source_format}")
print(f"Location:      {volume_file_path}")
print(f"Row count:     {row_count:,}")
print(f"Column count:  {col_count}")
print("=" * 60)

# COMMAND ----------

# Show schema preview
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
# MAGIC ## 5. Summary
# MAGIC
# MAGIC | Item | Status |
# MAGIC |------|--------|
# MAGIC | Configuration | Loaded from YAML |
# MAGIC | Schema | `main.food_intelligence` created |
# MAGIC | Volume | `raw_data` created for file storage |
# MAGIC | Data download | Complete |
# MAGIC | Validation | Row count and schema verified |
# MAGIC
# MAGIC **Next step**: Run `01_bronze_ingestion.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "Every pipeline I build starts with a configuration-driven setup notebook. Paths, thresholds,
# MAGIC > and quality rules live in YAML — not scattered across notebooks. On Databricks Free Edition
# MAGIC > I use Unity Catalog for governance — schemas for logical grouping, volumes for file storage,
# MAGIC > and managed Delta tables so I don't manage file paths manually. This maps directly to production
# MAGIC > Azure Databricks with Unity Catalog."
