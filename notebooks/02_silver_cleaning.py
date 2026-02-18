# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Cleaning
# MAGIC
# MAGIC **Purpose**: Transform raw Bronze data into a clean, trustworthy Silver table.
# MAGIC
# MAGIC This is the core data engineering notebook. It handles every real data quality issue
# MAGIC in the Open Food Facts dataset:
# MAGIC
# MAGIC 1. **Column Selection**: 200+ columns → ~40 useful ones
# MAGIC 2. **Deduplication**: Multiple contributors scan the same barcode → keep best record
# MAGIC 3. **Nutriment Flattening**: Nested JSON structs → flat typed columns
# MAGIC 4. **Category Standardization**: Multilingual taxonomy tags → clean English categories
# MAGIC 5. **Country Normalization**: `en:france` → `France`
# MAGIC 6. **Type Enforcement**: Strings → proper numeric/categorical types
# MAGIC 7. **Invalid Value Handling**: Negative calories, out-of-range scores → null with flags
# MAGIC
# MAGIC None of these are fabricated — every issue exists in the actual crowd-sourced data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Clean and transform data" — dedup, type casting, standardization  
# MAGIC **DP-700 Mapping**: "Design Silver layer" — flattening semi-structured data, data quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import yaml
import os
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    TimestampType,
    StringType,
)

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

# Unity Catalog references — managed tables, no DBFS paths
catalog = config["catalog"]
schema = config["schema"]
volume = config["volume"]
bronze_table = config["tables"]["bronze"]["products"]
silver_table = config["tables"]["silver"]["products"]
quality = config["quality"]

bronze_fqn = f"{catalog}.{schema}.{bronze_table}"
silver_fqn = f"{catalog}.{schema}.{silver_table}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Bronze source: {bronze_fqn}")
print(f"Silver target: {silver_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze
# MAGIC
# MAGIC We read from the Delta table, not the raw source. This is the medallion pattern —
# MAGIC each layer reads from the layer below it, never from the original source directly.

# COMMAND ----------

df_bronze = spark.table(bronze_fqn)
bronze_count = df_bronze.count()
bronze_cols = len(df_bronze.columns)

print(f"Bronze: {bronze_count:,} rows x {bronze_cols} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column Selection
# MAGIC
# MAGIC We go from 200+ columns down to ~40 useful ones. The rest are preserved in Bronze
# MAGIC for auditability. We drop:
# MAGIC - Debug/internal columns (`_keywords`, `debug_tags`, `*_prev_tags`)
# MAGIC - Sparse columns (>90% null)
# MAGIC - Redundant columns (e.g., both `brands` and `brands_tags` exist — we keep both
# MAGIC   because `brands` is human-readable and `brands_tags` is machine-parseable)
# MAGIC
# MAGIC The column list comes from `pipeline_config.yaml` — not hardcoded here.

# COMMAND ----------

# Build flat list of desired columns from config
silver_col_config = config["silver_columns"]
desired_columns = []
for group_name, cols in silver_col_config.items():
    desired_columns.extend(cols)

# Only select columns that actually exist in Bronze
# (Parquet vs JSONL may have different column sets)
available_columns = set(df_bronze.columns)
selected_columns = [c for c in desired_columns if c in available_columns]
missing_columns = [c for c in desired_columns if c not in available_columns]

print(f"Desired columns: {len(desired_columns)}")
print(f"Available in Bronze: {len(selected_columns)}")
if missing_columns:
    print(f"Missing (will be null): {missing_columns}")

df_selected = df_bronze.select(*selected_columns)
print(f"\nAfter column selection: {len(df_selected.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplication
# MAGIC
# MAGIC **The real problem**: Open Food Facts is crowd-sourced. Multiple people scan the same
# MAGIC barcode (same `code`) and create separate records. Some have better data than others.
# MAGIC
# MAGIC **Strategy**: Group by `code` (barcode), rank by:
# MAGIC 1. Highest `completeness` score (best data quality)
# MAGIC 2. Most recent `last_modified_datetime` (tie-breaker — freshest data wins)
# MAGIC
# MAGIC Then keep only rank 1 per barcode.
# MAGIC
# MAGIC **Why Window function, not groupBy**: `groupBy` would force us to pick an aggregation
# MAGIC for every column (first? max? min?). Window + row_number lets us keep the entire best
# MAGIC row intact.

# COMMAND ----------

# First, handle the case where completeness might be null
df_dedup_prep = df_selected.withColumn(
    "_completeness_safe",
    F.coalesce(F.col("completeness").cast(DoubleType()), F.lit(0.0)),
)

# Check if last_modified_datetime exists (may not be in all source formats)
has_modified_date = "last_modified_datetime" in df_dedup_prep.columns

# Window: partition by barcode, order by completeness DESC then freshness DESC (if available)
if has_modified_date:
    print("Using completeness + last_modified_datetime for dedup ranking")
    dedup_window = Window.partitionBy("code").orderBy(
        F.col("_completeness_safe").desc(),
        F.col("last_modified_datetime").desc_nulls_last(),
    )
else:
    print("last_modified_datetime not found — using completeness only for dedup")
    dedup_window = Window.partitionBy("code").orderBy(
        F.col("_completeness_safe").desc(),
    )

df_ranked = df_dedup_prep.withColumn("_dedup_rank", F.row_number().over(dedup_window))

# Keep only the best record per barcode
df_deduped = df_ranked.filter(F.col("_dedup_rank") == 1).drop(
    "_dedup_rank", "_completeness_safe"
)

deduped_count = df_deduped.count()
dupes_removed = bronze_count - deduped_count

print(f"Before dedup:  {bronze_count:,}")
print(f"After dedup:   {deduped_count:,}")
print(f"Dupes removed: {dupes_removed:,} ({dupes_removed / bronze_count * 100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Nutriment Flattening
# MAGIC
# MAGIC In the JSONL source, nutrition data lives in a nested `nutriments` struct with 100+
# MAGIC sub-fields. Even in the Parquet version, these arrive as top-level columns with
# MAGIC inconsistent naming (hyphens vs underscores).
# MAGIC
# MAGIC We standardize to clean column names and cast everything to `DoubleType`.
# MAGIC Some values arrive as strings like `"250 kcal"` — casting to Double handles the
# MAGIC numeric ones and nullifies the malformed strings (which is the correct behavior).

# COMMAND ----------

# Define the nutrition columns we want and their clean names
# Source column name → Silver column name
nutrition_mapping = {
    "energy-kcal_100g": "energy_kcal_100g",
    "energy_100g": "energy_kj_100g",
    "fat_100g": "fat_100g",
    "saturated-fat_100g": "saturated_fat_100g",
    "carbohydrates_100g": "carbohydrates_100g",
    "sugars_100g": "sugars_100g",
    "fiber_100g": "fiber_100g",
    "proteins_100g": "proteins_100g",
    "salt_100g": "salt_100g",
    "sodium_100g": "sodium_100g",
}

df_flat = df_deduped
for source_col, clean_col in nutrition_mapping.items():
    if source_col in df_flat.columns:
        # Cast to Double — strings like "250 kcal" become null (correct behavior)
        if source_col != clean_col:
            df_flat = df_flat.withColumn(
                clean_col, F.col(f"`{source_col}`").cast(DoubleType())
            ).drop(f"`{source_col}`")
        else:
            df_flat = df_flat.withColumn(
                clean_col, F.col(f"`{source_col}`").cast(DoubleType())
            )

print("Nutrition columns flattened and cast to DoubleType:")
for clean_col in nutrition_mapping.values():
    if clean_col in df_flat.columns:
        print(f"  {clean_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Invalid Value Handling
# MAGIC
# MAGIC Real data quality issues in Open Food Facts:
# MAGIC - **Negative nutritional values**: Data entry errors (e.g., -5g fat)
# MAGIC - **Impossible energy values**: >4000 kcal/100g (pure fat is ~900)
# MAGIC - **Zero energy for real products**: Sometimes a data entry omission
# MAGIC
# MAGIC Strategy: Set invalid values to null and flag with `_nutrition_was_corrected`.
# MAGIC We don't delete these rows — they may have valid data in other fields.

# COMMAND ----------

# Build correction conditions
nutrition_ranges = quality["nutrition_ranges"]

# Track which rows get corrected
correction_conditions = []

# Energy kcal range check
energy_min = nutrition_ranges["energy_kcal"]["min"]
energy_max = nutrition_ranges["energy_kcal"]["max"]
correction_conditions.append(
    (F.col("energy_kcal_100g") < energy_min)
    | (F.col("energy_kcal_100g") > energy_max)
)

# General nutrition range checks (fat, sugars, proteins, salt)
range_checks = {
    "fat_100g": nutrition_ranges["fat"],
    "sugars_100g": nutrition_ranges["sugars"],
    "proteins_100g": nutrition_ranges["proteins"],
    "salt_100g": nutrition_ranges["salt"],
}

for col_name, bounds in range_checks.items():
    if col_name in df_flat.columns:
        correction_conditions.append(
            (F.col(col_name) < bounds["min"]) | (F.col(col_name) > bounds["max"])
        )

# Combine all conditions: ANY invalid value triggers the flag
combined_condition = correction_conditions[0]
for cond in correction_conditions[1:]:
    combined_condition = combined_condition | cond

# Add correction flag
df_validated = df_flat.withColumn(
    "_nutrition_was_corrected",
    F.when(combined_condition, F.lit(True)).otherwise(F.lit(False)),
)

# Now null out the actual invalid values
df_validated = df_validated.withColumn(
    "energy_kcal_100g",
    F.when(
        (F.col("energy_kcal_100g") >= energy_min)
        & (F.col("energy_kcal_100g") <= energy_max),
        F.col("energy_kcal_100g"),
    ).otherwise(F.lit(None).cast(DoubleType())),
)

for col_name, bounds in range_checks.items():
    if col_name in df_validated.columns:
        df_validated = df_validated.withColumn(
            col_name,
            F.when(
                (F.col(col_name) >= bounds["min"])
                & (F.col(col_name) <= bounds["max"]),
                F.col(col_name),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )

# Count corrections
corrected_count = df_validated.filter(F.col("_nutrition_was_corrected")).count()
print(f"Rows with corrected nutrition values: {corrected_count:,} ({corrected_count / deduped_count * 100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Category Standardization
# MAGIC
# MAGIC `categories_tags` is an array like `["en:plant-based-foods","en:cereals","en:breakfast-cereals"]`.
# MAGIC The mess:
# MAGIC - Mixed language prefixes: `en:`, `fr:`, `de:`, `es:`, etc.
# MAGIC - Some have no prefix at all
# MAGIC - The last (most specific) tag is usually the most useful
# MAGIC
# MAGIC We extract:
# MAGIC - **`primary_category`**: The most specific English category (last `en:` tag, cleaned)
# MAGIC - **`category_depth`**: How deep in the taxonomy (useful for filtering specificity)

# COMMAND ----------

# categories_tags can be a string (comma-separated) or array depending on source format
# We handle both cases

# UDF to extract the most specific English category
@F.udf(StringType())
def extract_primary_category(categories):
    """Extract the most specific English category from the taxonomy tags."""
    if categories is None:
        return None

    # Handle both string and list inputs
    if isinstance(categories, str):
        tags = [t.strip() for t in categories.split(",")]
    else:
        tags = list(categories)

    # Filter to English tags and extract the category name
    en_tags = []
    for tag in tags:
        tag = tag.strip()
        if tag.startswith("en:"):
            # Remove prefix and clean: "en:breakfast-cereals" → "breakfast-cereals"
            en_tags.append(tag[3:].strip())
        elif ":" not in tag and tag:
            # No language prefix — assume English
            en_tags.append(tag.strip())

    if not en_tags:
        # Fall back to any tag if no English ones found
        for tag in tags:
            tag = tag.strip()
            if ":" in tag:
                en_tags.append(tag.split(":", 1)[1].strip())

    # Return the most specific (last) category
    return en_tags[-1] if en_tags else None


@F.udf(IntegerType())
def get_category_depth(categories):
    """Count taxonomy depth — how many levels deep this product is categorized."""
    if categories is None:
        return None
    if isinstance(categories, str):
        tags = [t.strip() for t in categories.split(",") if t.strip()]
    else:
        tags = [t for t in categories if t]
    return len(tags) if tags else None


df_categorized = df_validated.withColumns(
    {
        "primary_category": extract_primary_category(F.col("categories_tags")),
        "category_depth": get_category_depth(F.col("categories_tags")),
    }
)

# Show distribution
print("Top 15 primary categories:")
display(
    df_categorized.groupBy("primary_category")
    .count()
    .orderBy(F.col("count").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Country Normalization
# MAGIC
# MAGIC `countries_tags` contains values like `["en:france","en:united-states","en:germany"]`.
# MAGIC We extract:
# MAGIC - **`primary_country`**: First country listed, cleaned to title case (`France`)
# MAGIC - **`countries_count`**: How many countries this product is sold in

# COMMAND ----------

@F.udf(StringType())
def extract_primary_country(countries):
    """Extract the first country from countries_tags, cleaned to readable format."""
    if countries is None:
        return None

    if isinstance(countries, str):
        tags = [t.strip() for t in countries.split(",")]
    else:
        tags = list(countries)

    if not tags:
        return None

    # Take the first country
    first = tags[0].strip()

    # Remove language prefix: "en:france" → "france"
    if ":" in first:
        first = first.split(":", 1)[1]

    # Clean up: "united-states" → "United States"
    return first.replace("-", " ").strip().title()


@F.udf(IntegerType())
def count_countries(countries):
    """Count how many countries this product is sold in."""
    if countries is None:
        return None
    if isinstance(countries, str):
        tags = [t.strip() for t in countries.split(",") if t.strip()]
    else:
        tags = [t for t in countries if t]
    return len(tags) if tags else None


df_countries = df_categorized.withColumns(
    {
        "primary_country": extract_primary_country(F.col("countries_tags")),
        "countries_count": count_countries(F.col("countries_tags")),
    }
)

# Show distribution
print("Top 15 countries:")
display(
    df_countries.groupBy("primary_country")
    .count()
    .orderBy(F.col("count").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Type Enforcement & Score Validation
# MAGIC
# MAGIC **Nutri-Score**: Should be one of `a, b, c, d, e` (lowercase). Anything else → null.  
# MAGIC **NOVA group**: Should be 1, 2, 3, or 4 (integer). Anything else → null.  
# MAGIC **Eco-Score**: Same as Nutri-Score.  
# MAGIC **Dates**: Parse string timestamps to proper `TimestampType`.
# MAGIC
# MAGIC We validate against the allowed values in `pipeline_config.yaml`.

# COMMAND ----------

valid_nutriscore = quality["valid_nutriscore_grades"]  # ["a","b","c","d","e"]
valid_nova = quality["valid_nova_groups"]                # [1,2,3,4]
valid_ecoscore = quality["valid_ecoscore_grades"]        # ["a","b","c","d","e"]

df_typed = df_countries

# --- Nutri-Score: lowercase, validate against allowed values ---
if "nutriscore_grade" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "nutriscore_grade",
        F.when(
            F.lower(F.col("nutriscore_grade")).isin(valid_nutriscore),
            F.lower(F.col("nutriscore_grade")),
        ).otherwise(F.lit(None).cast(StringType())),
    )

# --- Nutri-Score numeric ---
if "nutriscore_score" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "nutriscore_score", F.col("nutriscore_score").cast(IntegerType())
    )

# --- NOVA group: cast to int, validate range ---
if "nova_group" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "nova_group",
        F.when(
            F.col("nova_group").cast(IntegerType()).isin(valid_nova),
            F.col("nova_group").cast(IntegerType()),
        ).otherwise(F.lit(None).cast(IntegerType())),
    )

# --- Eco-Score: same pattern as Nutri-Score ---
if "ecoscore_grade" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "ecoscore_grade",
        F.when(
            F.lower(F.col("ecoscore_grade")).isin(valid_ecoscore),
            F.lower(F.col("ecoscore_grade")),
        ).otherwise(F.lit(None).cast(StringType())),
    )

if "ecoscore_score" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "ecoscore_score", F.col("ecoscore_score").cast(IntegerType())
    )

# --- Dates: parse to TimestampType ---
for date_col in ["created_datetime", "last_modified_datetime"]:
    if date_col in df_typed.columns:
        df_typed = df_typed.withColumn(
            date_col, F.to_timestamp(F.col(date_col))
        )

# --- Completeness: ensure it's a proper double ---
if "completeness" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "completeness", F.col("completeness").cast(DoubleType())
    )

# --- Additives count: ensure integer ---
if "additives_n" in df_typed.columns:
    df_typed = df_typed.withColumn(
        "additives_n", F.col("additives_n").cast(IntegerType())
    )

print("Type enforcement complete.")
print("\nScore validation summary:")
for score_col, valid_vals in [
    ("nutriscore_grade", valid_nutriscore),
    ("nova_group", valid_nova),
    ("ecoscore_grade", valid_ecoscore),
]:
    if score_col in df_typed.columns:
        non_null = df_typed.filter(F.col(score_col).isNotNull()).count()
        pct = non_null / deduped_count * 100
        print(f"  {score_col:<25} {non_null:>10,} valid ({pct:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Drop Empty Rows
# MAGIC
# MAGIC Some records have a barcode but literally nothing else — no name, no nutrition,
# MAGIC no category. These are useless even in Silver. We drop rows where `code` is null
# MAGIC or empty (the barcode is our primary key — without it, the row is unidentifiable).

# COMMAND ----------

df_clean = df_typed.filter(
    F.col("code").isNotNull() & (F.trim(F.col("code")) != "")
)

dropped_empty = deduped_count - df_clean.count()
silver_count = df_clean.count()
print(f"Dropped {dropped_empty:,} rows with null/empty barcode")
print(f"Silver row count: {silver_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Write Silver Delta Table
# MAGIC
# MAGIC Key difference from Bronze:
# MAGIC - We **partition by `primary_country`** — most Gold queries filter by country,
# MAGIC   so partitioning means Spark only scans relevant files (partition pruning).
# MAGIC - Schema is now clean and enforced — no more surprise types.
# MAGIC - We use Unity Catalog **managed tables** — no explicit DBFS paths. The catalog
# MAGIC   manages storage, metadata, and access control in one place.

# COMMAND ----------

partition_col = config["partitioning"]["silver"]["partition_by"]

writer = (
    df_clean.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
)

# Apply partitioning if configured
if partition_col and partition_col in df_clean.columns:
    writer = writer.partitionBy(partition_col)
    print(f"Partitioning Silver by: {partition_col}")

writer.saveAsTable(silver_fqn)
print(f"Silver table written: {silver_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validation & Summary Stats

# COMMAND ----------

df_silver = spark.table(silver_fqn)
final_count = df_silver.count()
final_cols = len(df_silver.columns)

# Verify no duplicate barcodes
dupe_check = (
    df_silver.groupBy("code")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("=" * 70)
print("SILVER CLEANING SUMMARY")
print("=" * 70)
print(f"Bronze input:          {bronze_count:>12,} rows x {bronze_cols} columns")
print(f"After column select:   {' ':>12}       {len(selected_columns)} columns")
print(f"After dedup:           {deduped_count:>12,} rows  (-{dupes_removed:,} dupes)")
print(f"After validation:      {silver_count:>12,} rows  (-{dropped_empty:,} empty)")
print(f"Final Silver:          {final_count:>12,} rows x {final_cols} columns")
print(f"Duplicate barcodes:    {dupe_check:>12}   {'PASS' if dupe_check == 0 else 'FAIL'}")
print(f"Nutrition corrections: {corrected_count:>12,} rows")
if partition_col:
    partition_count = df_silver.select(partition_col).distinct().count()
    print(f"Partitions ({partition_col}): {partition_count:>7,}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Column-level Quality Report

# COMMAND ----------

print("Silver column quality report:\n")
print(f"{'Column':<35} {'Type':<15} {'Non-null':>10} {'% Fill':>8}")
print("-" * 70)

for field in df_silver.schema.fields:
    non_null = df_silver.filter(F.col(field.name).isNotNull()).count()
    fill_pct = (non_null / final_count) * 100
    dtype = str(field.dataType).replace("Type", "")
    print(f"{field.name:<35} {dtype:<15} {non_null:>10,} {fill_pct:>7.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Save Silver Metadata

# COMMAND ----------

silver_metadata = {
    "bronze_input_rows": bronze_count,
    "duplicates_removed": dupes_removed,
    "empty_rows_dropped": dropped_empty,
    "nutrition_corrections": corrected_count,
    "silver_output_rows": final_count,
    "silver_columns": final_cols,
    "duplicate_barcodes_remaining": dupe_check,
    "partition_column": partition_col,
    "cleaning_timestamp": datetime.now(timezone.utc).isoformat(),
    "table_name": silver_fqn,
}

# Write metadata to a Unity Catalog Volume (replaces dbutils.fs.put to DBFS)
metadata_dir = f"/Volumes/{catalog}/{schema}/{volume}/metadata"
os.makedirs(metadata_dir, exist_ok=True)

metadata_file = os.path.join(metadata_dir, "silver_cleaning_metadata.json")
with open(metadata_file, "w") as f:
    json.dump(silver_metadata, f, indent=2)

print(f"Silver metadata saved to: {metadata_file}")
print(json.dumps(silver_metadata, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "Silver is where I solve every real data quality issue in Open Food Facts. Deduplication
# MAGIC > uses a Window function partitioned by barcode, ranking by completeness score then freshness —
# MAGIC > this keeps the best record per product from crowd-sourced duplicates. I flatten nested
# MAGIC > nutriment structs, cast strings to doubles, validate Nutri-Score grades against an allowed-value
# MAGIC > list, and standardize multilingual category tags into clean English taxonomy. Invalid nutrition
# MAGIC > values get nulled but flagged — I never silently delete data. The table is partitioned by
# MAGIC > country because most Gold queries filter by region, enabling partition pruning."
