# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Enrichment
# MAGIC
# MAGIC **Purpose**: Add derived columns to the clean Silver table that don't exist in the source.
# MAGIC
# MAGIC Cleaning (notebook 02) fixed problems. Enrichment adds business value:
# MAGIC - **`nutrition_completeness`**: What % of the 8 core nutrients are filled in?
# MAGIC - **`is_ultra_processed`**: Boolean flag from NOVA group (saves Gold from re-deriving)
# MAGIC - **`health_tier`**: "Healthy" / "Moderate" / "Unhealthy" from Nutri-Score
# MAGIC - **`data_quality_score`**: Composite score — how trustworthy is this record overall?
# MAGIC - **`product_age_days`**: Days since product was first added to Open Food Facts
# MAGIC
# MAGIC These columns are pre-computed in Silver so every Gold notebook doesn't have to
# MAGIC re-derive them independently — a DRY (Don't Repeat Yourself) data principle.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Create measures and metrics" — derived business columns  
# MAGIC **DP-700 Mapping**: "Design Silver layer" — enrichment patterns, computed columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import yaml
import os
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

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

db_name = config["database"]["name"]
silver_path = config["delta_tables"]["silver"]["products"]

spark.sql(f"USE {db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Clean Silver Table

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)
silver_count = df_silver.count()
print(f"Silver input: {silver_count:,} rows x {len(df_silver.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Nutrition Completeness Score
# MAGIC
# MAGIC Of the 8 core nutrients (energy, fat, saturated fat, carbs, sugars, fiber, proteins, salt),
# MAGIC what percentage are non-null for this product?
# MAGIC
# MAGIC This matters because ~40% of products in Open Food Facts have incomplete nutrition panels.
# MAGIC Gold aggregations on nutrition data should weight or filter by this score.

# COMMAND ----------

core_nutrients = [
    "energy_kcal_100g",
    "fat_100g",
    "saturated_fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "fiber_100g",
    "proteins_100g",
    "salt_100g",
]

# Filter to only nutrients that exist in the dataset
available_nutrients = [n for n in core_nutrients if n in df_silver.columns]

# Count non-null nutrients per row, divide by total
non_null_count = sum(
    F.when(F.col(n).isNotNull(), F.lit(1)).otherwise(F.lit(0))
    for n in available_nutrients
)

df_enriched = df_silver.withColumn(
    "nutrition_completeness",
    (non_null_count / F.lit(len(available_nutrients))).cast(DoubleType()),
)

# Show distribution
print(f"Nutrition completeness (based on {len(available_nutrients)} core nutrients):\n")
display(
    df_enriched.groupBy(
        F.round(F.col("nutrition_completeness"), 1).alias("completeness_bucket")
    )
    .count()
    .orderBy("completeness_bucket")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ultra-Processed Flag
# MAGIC
# MAGIC NOVA group 4 = ultra-processed food. This is a key metric for health regulators and
# MAGIC a common Gold query filter. Pre-computing it in Silver avoids `WHERE nova_group = 4`
# MAGIC scattered across every downstream notebook.

# COMMAND ----------

df_enriched = df_enriched.withColumn(
    "is_ultra_processed",
    F.when(F.col("nova_group") == 4, F.lit(True))
    .when(F.col("nova_group").isNotNull(), F.lit(False))
    .otherwise(F.lit(None))  # null nova_group → null flag (unknown)
)

# Stats
ultra_stats = df_enriched.groupBy("is_ultra_processed").count().collect()
print("Ultra-processed distribution:")
for row in ultra_stats:
    label = {True: "Ultra-processed (NOVA 4)", False: "Not ultra-processed", None: "Unknown (null)"}
    print(f"  {label.get(row['is_ultra_processed'], 'Unknown'):<35} {row['count']:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Health Tier
# MAGIC
# MAGIC A simplified classification derived from Nutri-Score:
# MAGIC - **Healthy**: Nutri-Score A or B
# MAGIC - **Moderate**: Nutri-Score C
# MAGIC - **Unhealthy**: Nutri-Score D or E
# MAGIC - **Unknown**: No Nutri-Score available
# MAGIC
# MAGIC This is useful for dashboards where a 3-tier traffic light is more intuitive
# MAGIC than a 5-letter grade for non-technical stakeholders.

# COMMAND ----------

df_enriched = df_enriched.withColumn(
    "health_tier",
    F.when(F.col("nutriscore_grade").isin("a", "b"), F.lit("Healthy"))
    .when(F.col("nutriscore_grade") == "c", F.lit("Moderate"))
    .when(F.col("nutriscore_grade").isin("d", "e"), F.lit("Unhealthy"))
    .otherwise(F.lit(None))
)

# Stats
print("Health tier distribution:")
display(
    df_enriched.groupBy("health_tier")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality Score
# MAGIC
# MAGIC A composite score (0.0 to 1.0) reflecting how complete and trustworthy this record is.
# MAGIC Weights the presence of key fields:
# MAGIC - Product name: 15%
# MAGIC - Brand: 10%
# MAGIC - Category: 15%
# MAGIC - Country: 10%
# MAGIC - Nutri-Score: 15%
# MAGIC - NOVA group: 10%
# MAGIC - Nutrition completeness: 25% (already computed above)
# MAGIC
# MAGIC Gold notebooks can use this to filter to "high-quality records only" for analysis.

# COMMAND ----------

# Define quality components and their weights
quality_components = {
    "product_name": 0.15,
    "brands": 0.10,
    "primary_category": 0.15,
    "primary_country": 0.10,
    "nutriscore_grade": 0.15,
    "nova_group": 0.10,
}

# Build the weighted score
quality_expr = F.lit(0.0)
for col_name, weight in quality_components.items():
    if col_name in df_enriched.columns:
        quality_expr = quality_expr + F.when(
            F.col(col_name).isNotNull() & (F.col(col_name) != ""),
            F.lit(weight),
        ).otherwise(F.lit(0.0))

# Add nutrition completeness component (25% weight)
quality_expr = quality_expr + (
    F.coalesce(F.col("nutrition_completeness"), F.lit(0.0)) * F.lit(0.25)
)

df_enriched = df_enriched.withColumn(
    "data_quality_score", F.round(quality_expr, 3)
)

# Show distribution
print("Data quality score distribution:")
display(
    df_enriched.groupBy(
        F.round(F.col("data_quality_score"), 1).alias("quality_bucket")
    )
    .count()
    .orderBy("quality_bucket")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Product Age
# MAGIC
# MAGIC Days since the product was first added to Open Food Facts.
# MAGIC Useful for trend analysis: are newer products healthier than older ones?

# COMMAND ----------

df_enriched = df_enriched.withColumn(
    "product_age_days",
    F.when(
        F.col("created_datetime").isNotNull(),
        F.datediff(F.current_date(), F.col("created_datetime")),
    ).otherwise(F.lit(None)),
)

# Stats
print("Product age statistics:")
display(
    df_enriched.select(
        F.min("product_age_days").alias("min_age_days"),
        F.max("product_age_days").alias("max_age_days"),
        F.avg("product_age_days").alias("avg_age_days"),
        F.percentile_approx("product_age_days", 0.5).alias("median_age_days"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Enriched Silver Table
# MAGIC
# MAGIC We overwrite the Silver table with the enriched version.
# MAGIC Same partitioning strategy as before.

# COMMAND ----------

partition_col = config["partitioning"]["silver"]["partition_by"]

writer = (
    df_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
)

if partition_col and partition_col in df_enriched.columns:
    writer = writer.partitionBy(partition_col)
    print(f"Partitioning by: {partition_col}")

writer.save(silver_path)
print(f"Enriched Silver written to: {silver_path}")

# Refresh metastore registration
spark.sql(f"REFRESH TABLE {db_name}.silver_products")
print(f"Table refreshed: {db_name}.silver_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Final Validation

# COMMAND ----------

df_final = spark.read.format("delta").load(silver_path)
final_count = df_final.count()
final_cols = len(df_final.columns)

# Verify row count hasn't changed (enrichment adds columns, not rows)
assert final_count == silver_count, (
    f"Row count changed! Before: {silver_count:,}, After: {final_count:,}"
)

# List the new enrichment columns
enrichment_cols = [
    "nutrition_completeness",
    "is_ultra_processed",
    "health_tier",
    "data_quality_score",
    "product_age_days",
]

print("=" * 70)
print("SILVER ENRICHMENT SUMMARY")
print("=" * 70)
print(f"Input rows:      {silver_count:>12,}")
print(f"Output rows:     {final_count:>12,}  (unchanged — enrichment adds columns only)")
print(f"Total columns:   {final_cols:>12}")
print(f"\nNew enrichment columns:")
for col in enrichment_cols:
    if col in df_final.columns:
        non_null = df_final.filter(F.col(col).isNotNull()).count()
        pct = (non_null / final_count) * 100
        print(f"  {col:<30} {non_null:>10,} non-null ({pct:.1f}%)")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Save Enrichment Metadata

# COMMAND ----------

enrichment_metadata = {
    "input_rows": silver_count,
    "output_rows": final_count,
    "enrichment_columns_added": enrichment_cols,
    "total_columns": final_cols,
    "enrichment_timestamp": datetime.now(timezone.utc).isoformat(),
    "delta_path": silver_path,
    "table_name": f"{db_name}.silver_products",
}

metadata_path = "/FileStore/food-intelligence/metadata/silver_enrichment_metadata.json"
dbutils.fs.put(metadata_path, json.dumps(enrichment_metadata, indent=2), overwrite=True)

print(f"Enrichment metadata saved to: {metadata_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "I separate Silver into two stages: cleaning and enrichment. Cleaning fixes data problems —
# MAGIC > dedup, type casting, invalid value handling. Enrichment adds business value — derived columns
# MAGIC > like `nutrition_completeness`, `health_tier`, and `data_quality_score`. The separation matters
# MAGIC > because cleaning logic rarely changes (dedup rules are stable), but enrichment evolves with
# MAGIC > business needs (maybe next quarter we add a 'sustainability_tier' from Eco-Score). Keeping them
# MAGIC > in separate notebooks means I can re-run enrichment without re-running the expensive dedup."
