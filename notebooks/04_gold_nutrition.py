# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: Nutrition Analytics
# MAGIC
# MAGIC **Purpose**: Build pre-aggregated nutrition tables for analysts and dashboards.
# MAGIC
# MAGIC Three Gold tables:
# MAGIC 1. **`gold_nutrition_by_category`**: Avg calories, fat, sugar, protein per food category
# MAGIC 2. **`gold_brand_scorecard`**: Per-brand nutrition profile + health scores
# MAGIC 3. **`gold_category_comparison`**: Side-by-side nutrition for competing categories
# MAGIC
# MAGIC These answer questions like: "Are granola bars actually healthier than candy bars?"
# MAGIC and "Which brands consistently score well on nutrition?"
# MAGIC
# MAGIC ---
# MAGIC

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

# Unity Catalog references
catalog = config["catalog"]
schema = config["schema"]
silver_table = config["tables"]["silver"]["products"]
gold_tables = config["tables"]["gold"]
quality = config["quality"]

min_per_category = quality["min_products_per_category"]  # 50
min_per_brand = quality["min_products_per_brand"]          # 20

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver

# COMMAND ----------

# Read from Unity Catalog managed table instead of DBFS path
df_silver = spark.table(f"{catalog}.{schema}.{silver_table}")
silver_count = df_silver.count()
print(f"Silver input: {silver_count:,} rows")

# Get list of available columns for dynamic aggregation
silver_columns = df_silver.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Table 1: Nutrition by Category
# MAGIC
# MAGIC Average nutritional values per food category. Only includes categories with at least
# MAGIC `min_products_per_category` (50) products to prevent misleading statistics.
# MAGIC
# MAGIC We also include `nutrition_completeness_avg` so consumers of this table know
# MAGIC how much to trust the nutrition averages — a category where 80% of products
# MAGIC have complete nutrition data is more reliable than one at 20%.

# COMMAND ----------

# Build dynamic aggregation list based on available columns
agg_exprs = [F.count("*").alias("product_count")]

# Core nutrition averages - only add if column exists
nutrition_cols_avg = [
    ("energy_kcal_100g", "avg_energy_kcal", 1),
    ("fat_100g", "avg_fat_g", 1),
    ("saturated_fat_100g", "avg_saturated_fat_g", 1),
    ("carbohydrates_100g", "avg_carbohydrates_g", 1),
    ("sugars_100g", "avg_sugars_g", 1),
    ("fiber_100g", "avg_fiber_g", 1),
    ("proteins_100g", "avg_proteins_g", 1),
    ("salt_100g", "avg_salt_g", 2),
]
for col_name, alias, precision in nutrition_cols_avg:
    if col_name in silver_columns:
        agg_exprs.append(F.round(F.avg(col_name), precision).alias(alias))

# Standard deviations - only add if corresponding avg column exists
stddev_cols = [
    ("energy_kcal_100g", "stddev_energy_kcal"),
    ("sugars_100g", "stddev_sugars_g"),
]
for col_name, alias in stddev_cols:
    if col_name in silver_columns:
        agg_exprs.append(F.round(F.stddev(col_name), 1).alias(alias))

# Quality indicators - always include
agg_exprs.append(F.round(F.avg("nutrition_completeness"), 3).alias("avg_nutrition_completeness"))
agg_exprs.append(F.round(F.avg("data_quality_score"), 3).alias("avg_data_quality_score"))

# Health score distribution - check nutriscore exists
if "nutriscore_grade" in silver_columns:
    agg_exprs.append(
        F.round(F.avg(F.when(F.col("nutriscore_grade").isNotNull(), 1).otherwise(0)), 3)
        .alias("pct_with_nutriscore")
    )

df_nutrition_by_cat = (
    df_silver
    .filter(F.col("primary_category").isNotNull())
    .groupBy("primary_category")
    .agg(*agg_exprs)
    # Filter to categories with enough products for meaningful stats
    .filter(F.col("product_count") >= min_per_category)
    .orderBy(F.col("product_count").desc())
)

cat_count = df_nutrition_by_cat.count()
print(f"Categories with >= {min_per_category} products: {cat_count}")

# Write as Unity Catalog managed table
df_nutrition_by_cat.write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.{gold_tables['nutrition_by_category']}")

print(f"Written: {catalog}.{schema}.{gold_tables['nutrition_by_category']} ({cat_count} categories)")

# Preview top categories
display(df_nutrition_by_cat.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Table 2: Brand Scorecard
# MAGIC
# MAGIC Per-brand report card: average Nutri-Score, NOVA group, nutrition profile, product count.
# MAGIC This answers: "Is Brand X actually healthier than Brand Y across their product lines?"
# MAGIC
# MAGIC We convert letter grades to numeric for averaging:
# MAGIC - Nutri-Score: a=1, b=2, c=3, d=4, e=5 (lower is healthier)
# MAGIC - Then convert back to a letter for the "average grade"

# COMMAND ----------

# Map nutriscore letter to numeric for averaging
nutriscore_numeric = (
    F.when(F.col("nutriscore_grade") == "a", 1)
    .when(F.col("nutriscore_grade") == "b", 2)
    .when(F.col("nutriscore_grade") == "c", 3)
    .when(F.col("nutriscore_grade") == "d", 4)
    .when(F.col("nutriscore_grade") == "e", 5)
)

# Map average numeric back to letter grade
def avg_to_grade(col_name):
    """Convert average numeric score back to a letter grade."""
    return (
        F.when(F.col(col_name) <= 1.5, "a")
        .when(F.col(col_name) <= 2.5, "b")
        .when(F.col(col_name) <= 3.5, "c")
        .when(F.col(col_name) <= 4.5, "d")
        .otherwise("e")
    )

# Build dynamic aggregation for brand scorecard
brand_agg_exprs = [F.count("*").alias("product_count")]

# Health scores - nutriscore if available
if "nutriscore_grade" in silver_columns:
    brand_agg_exprs.append(F.round(F.avg("nutriscore_numeric"), 2).alias("avg_nutriscore_numeric"))

# NOVA group if available
if "nova_group" in silver_columns:
    brand_agg_exprs.append(F.round(F.avg(F.col("nova_group").cast(DoubleType())), 2).alias("avg_nova_group"))

# Nutrition profile - only add if columns exist
brand_nutrition_cols = [
    ("energy_kcal_100g", "avg_energy_kcal", 1),
    ("sugars_100g", "avg_sugars_g", 1),
    ("fat_100g", "avg_fat_g", 1),
    ("proteins_100g", "avg_proteins_g", 1),
    ("fiber_100g", "avg_fiber_g", 1),
    ("salt_100g", "avg_salt_g", 2),
]
for col_name, alias, precision in brand_nutrition_cols:
    if col_name in silver_columns:
        brand_agg_exprs.append(F.round(F.avg(col_name), precision).alias(alias))

# Ultra-processed percentage
if "is_ultra_processed" in silver_columns:
    brand_agg_exprs.append(
        F.round(
            F.avg(F.when(F.col("is_ultra_processed") == True, 1).otherwise(0)), 3  # noqa: E712
        ).alias("pct_ultra_processed")
    )

# Health tier distribution
if "health_tier" in silver_columns:
    brand_agg_exprs.extend([
        F.round(
            F.avg(F.when(F.col("health_tier") == "Healthy", 1).otherwise(0)), 3
        ).alias("pct_healthy"),
        F.round(
            F.avg(F.when(F.col("health_tier") == "Unhealthy", 1).otherwise(0)), 3
        ).alias("pct_unhealthy"),
    ])

# Data quality - always include
brand_agg_exprs.append(F.round(F.avg("data_quality_score"), 3).alias("avg_data_quality"))

df_brand_scorecard = (
    df_silver
    .filter(F.col("brands").isNotNull() & (F.trim(F.col("brands")) != ""))
    .withColumn("nutriscore_numeric", nutriscore_numeric)
    .groupBy("brands")
    .agg(*brand_agg_exprs)
    .filter(F.col("product_count") >= min_per_brand)
)

# Add the average grade letter only if nutriscore was calculated
if "avg_nutriscore_numeric" in df_brand_scorecard.columns:
    df_brand_scorecard = df_brand_scorecard.withColumn("avg_nutriscore_grade", avg_to_grade("avg_nutriscore_numeric"))

df_brand_scorecard = df_brand_scorecard.orderBy(F.col("product_count").desc())

brand_count = df_brand_scorecard.count()
print(f"Brands with >= {min_per_brand} products: {brand_count}")

# Write as Unity Catalog managed table
df_brand_scorecard.write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.{gold_tables['brand_scorecard']}")

print(f"Written: {catalog}.{schema}.{gold_tables['brand_scorecard']} ({brand_count} brands)")

# Preview top brands by product count
display(df_brand_scorecard.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Table 3: Category Comparison
# MAGIC
# MAGIC Side-by-side nutrition comparison for competing categories.
# MAGIC Example use case: "How do breakfast cereals compare to granola vs oatmeal?"
# MAGIC
# MAGIC This is the same data as `nutrition_by_category` but restructured for
# MAGIC comparison dashboards — pivoted so you can put categories in columns.
# MAGIC We also add rank columns so analysts can quickly find the healthiest/worst categories.

# COMMAND ----------

# Start from the nutrition_by_category table we just built
# Add rank columns only if corresponding nutrition columns exist
comparison_columns = df_nutrition_by_cat.columns
rank_columns = []

df_comparison = df_nutrition_by_cat

# Sugar rank
if "avg_sugars_g" in comparison_columns:
    df_comparison = df_comparison.withColumn(
        "sugar_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_sugars_g").asc_nulls_last())
        ),
    )
    rank_columns.append("sugar_rank")

# Calorie rank
if "avg_energy_kcal" in comparison_columns:
    df_comparison = df_comparison.withColumn(
        "calorie_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_energy_kcal").asc_nulls_last())
        ),
    )
    rank_columns.append("calorie_rank")

# Protein rank
if "avg_proteins_g" in comparison_columns:
    df_comparison = df_comparison.withColumn(
        "protein_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_proteins_g").desc_nulls_last())
        ),
    )
    rank_columns.append("protein_rank")

# Fiber rank
if "avg_fiber_g" in comparison_columns:
    df_comparison = df_comparison.withColumn(
        "fiber_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_fiber_g").desc_nulls_last())
        ),
    )
    rank_columns.append("fiber_rank")

# Composite health rank: only if at least one rank exists
if rank_columns:
    rank_sum = F.col(rank_columns[0])
    for col in rank_columns[1:]:
        rank_sum = rank_sum + F.col(col)
    df_comparison = df_comparison.withColumn(
        "overall_health_rank",
        F.dense_rank().over(
            F.Window.orderBy(rank_sum.asc())
        ),
    )
    df_comparison = df_comparison.orderBy("overall_health_rank")

comparison_count = df_comparison.count()

# Write as Unity Catalog managed table
df_comparison.write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.{gold_tables['category_comparison']}")

print(f"Written: {catalog}.{schema}.{gold_tables['category_comparison']} ({comparison_count} categories)")

# Show healthiest categories
print("\nTop 15 healthiest categories (by composite rank):")

# Build select list based on available columns
comparison_columns = df_comparison.columns
select_cols = ["primary_category", "product_count"]

# Add nutrition columns if they exist
for col in ["avg_energy_kcal", "avg_sugars_g", "avg_proteins_g", "avg_fiber_g"]:
    if col in comparison_columns:
        select_cols.append(col)

# Always include rank
if "overall_health_rank" in comparison_columns:
    select_cols.append("overall_health_rank")

display(df_comparison.select(*select_cols).limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 70)
print("GOLD NUTRITION ANALYTICS SUMMARY")
print("=" * 70)
print(f"  {gold_tables['nutrition_by_category']}:  {cat_count:>6} categories (min {min_per_category} products)")
print(f"  {gold_tables['brand_scorecard']}:        {brand_count:>6} brands (min {min_per_brand} products)")
print(f"  {gold_tables['category_comparison']}:    {comparison_count:>6} categories with health ranks")
print(f"  Catalog: {catalog} | Schema: {schema}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "Gold tables are pre-aggregated for specific business questions. `nutrition_by_category`
# MAGIC > tells health regulators which food categories have the highest sugar content.
# MAGIC > `brand_scorecard` lets competitive analysts compare brands on health metrics — I convert
# MAGIC > Nutri-Score letters to numeric, average them, and convert back, so a brand that's mostly
# MAGIC > A's and B's gets a meaningful 'b' grade, not a useless 'a,b,a,c' string. All Gold tables
# MAGIC > enforce minimum sample sizes from config to prevent misleading stats from tiny categories.
# MAGIC > Everything is registered as Unity Catalog managed tables — no DBFS paths to manage,
# MAGIC > governance and lineage come for free, and any downstream consumer can discover these
# MAGIC > tables through the catalog without needing to know storage locations."
