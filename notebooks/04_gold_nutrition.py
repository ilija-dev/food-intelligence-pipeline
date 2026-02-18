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
# MAGIC **DP-600 Mapping**: "Create measures and metrics" — business aggregations  
# MAGIC **DP-700 Mapping**: "Build consumption layer" — Gold table design patterns

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

# Load config
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

db_name = config["database"]["name"]
silver_path = config["delta_tables"]["silver"]["products"]
gold_paths = config["delta_tables"]["gold"]
quality = config["quality"]

min_per_category = quality["min_products_per_category"]  # 50
min_per_brand = quality["min_products_per_brand"]          # 20

spark.sql(f"USE {db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)
silver_count = df_silver.count()
print(f"Silver input: {silver_count:,} rows")

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

df_nutrition_by_cat = (
    df_silver
    .filter(F.col("primary_category").isNotNull())
    .groupBy("primary_category")
    .agg(
        F.count("*").alias("product_count"),

        # Core nutrition averages
        F.round(F.avg("energy_kcal_100g"), 1).alias("avg_energy_kcal"),
        F.round(F.avg("fat_100g"), 1).alias("avg_fat_g"),
        F.round(F.avg("saturated_fat_100g"), 1).alias("avg_saturated_fat_g"),
        F.round(F.avg("carbohydrates_100g"), 1).alias("avg_carbohydrates_g"),
        F.round(F.avg("sugars_100g"), 1).alias("avg_sugars_g"),
        F.round(F.avg("fiber_100g"), 1).alias("avg_fiber_g"),
        F.round(F.avg("proteins_100g"), 1).alias("avg_proteins_g"),
        F.round(F.avg("salt_100g"), 2).alias("avg_salt_g"),

        # Standard deviations — high stddev means wide variation within category
        F.round(F.stddev("energy_kcal_100g"), 1).alias("stddev_energy_kcal"),
        F.round(F.stddev("sugars_100g"), 1).alias("stddev_sugars_g"),

        # Quality indicators
        F.round(F.avg("nutrition_completeness"), 3).alias("avg_nutrition_completeness"),
        F.round(F.avg("data_quality_score"), 3).alias("avg_data_quality_score"),

        # Health score distribution within category
        F.round(F.avg(F.when(F.col("nutriscore_grade").isNotNull(), 1).otherwise(0)), 3)
        .alias("pct_with_nutriscore"),
    )
    # Filter to categories with enough products for meaningful stats
    .filter(F.col("product_count") >= min_per_category)
    .orderBy(F.col("product_count").desc())
)

cat_count = df_nutrition_by_cat.count()
print(f"Categories with >= {min_per_category} products: {cat_count}")

# Write to Delta
df_nutrition_by_cat.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).save(gold_paths["nutrition_by_category"])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.gold_nutrition_by_category
    USING DELTA LOCATION '{gold_paths["nutrition_by_category"]}'
""")

print(f"Written: {db_name}.gold_nutrition_by_category ({cat_count} categories)")

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

df_brand_scorecard = (
    df_silver
    .filter(F.col("brands").isNotNull() & (F.trim(F.col("brands")) != ""))
    .withColumn("nutriscore_numeric", nutriscore_numeric)
    .groupBy("brands")
    .agg(
        F.count("*").alias("product_count"),

        # Health scores
        F.round(F.avg("nutriscore_numeric"), 2).alias("avg_nutriscore_numeric"),
        F.round(F.avg(F.col("nova_group").cast(DoubleType())), 2).alias("avg_nova_group"),

        # Nutrition profile
        F.round(F.avg("energy_kcal_100g"), 1).alias("avg_energy_kcal"),
        F.round(F.avg("sugars_100g"), 1).alias("avg_sugars_g"),
        F.round(F.avg("fat_100g"), 1).alias("avg_fat_g"),
        F.round(F.avg("proteins_100g"), 1).alias("avg_proteins_g"),
        F.round(F.avg("fiber_100g"), 1).alias("avg_fiber_g"),
        F.round(F.avg("salt_100g"), 2).alias("avg_salt_g"),

        # Ultra-processed percentage
        F.round(
            F.avg(F.when(F.col("is_ultra_processed") == True, 1).otherwise(0)), 3  # noqa: E712
        ).alias("pct_ultra_processed"),

        # Health tier distribution
        F.round(
            F.avg(F.when(F.col("health_tier") == "Healthy", 1).otherwise(0)), 3
        ).alias("pct_healthy"),
        F.round(
            F.avg(F.when(F.col("health_tier") == "Unhealthy", 1).otherwise(0)), 3
        ).alias("pct_unhealthy"),

        # Data quality
        F.round(F.avg("data_quality_score"), 3).alias("avg_data_quality"),
    )
    .filter(F.col("product_count") >= min_per_brand)
    # Add the average grade letter
    .withColumn("avg_nutriscore_grade", avg_to_grade("avg_nutriscore_numeric"))
    .orderBy(F.col("product_count").desc())
)

brand_count = df_brand_scorecard.count()
print(f"Brands with >= {min_per_brand} products: {brand_count}")

# Write to Delta
df_brand_scorecard.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).save(gold_paths["brand_scorecard"])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.gold_brand_scorecard
    USING DELTA LOCATION '{gold_paths["brand_scorecard"]}'
""")

print(f"Written: {db_name}.gold_brand_scorecard ({brand_count} brands)")

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
df_comparison = (
    df_nutrition_by_cat
    .withColumn(
        "sugar_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_sugars_g").asc_nulls_last())
        ),
    )
    .withColumn(
        "calorie_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_energy_kcal").asc_nulls_last())
        ),
    )
    .withColumn(
        "protein_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_proteins_g").desc_nulls_last())
        ),
    )
    .withColumn(
        "fiber_rank",
        F.dense_rank().over(
            F.Window.orderBy(F.col("avg_fiber_g").desc_nulls_last())
        ),
    )
    # Composite health rank: low sugar + low calories + high protein + high fiber
    .withColumn(
        "overall_health_rank",
        F.dense_rank().over(
            F.Window.orderBy(
                (
                    F.col("sugar_rank")
                    + F.col("calorie_rank")
                    + F.col("protein_rank")
                    + F.col("fiber_rank")
                ).asc()
            )
        ),
    )
    .orderBy("overall_health_rank")
)

comparison_count = df_comparison.count()

# Write to Delta
df_comparison.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).save(gold_paths["category_comparison"])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.gold_category_comparison
    USING DELTA LOCATION '{gold_paths["category_comparison"]}'
""")

print(f"Written: {db_name}.gold_category_comparison ({comparison_count} categories)")

# Show healthiest categories
print("\nTop 15 healthiest categories (by composite rank):")
display(
    df_comparison.select(
        "primary_category",
        "product_count",
        "avg_energy_kcal",
        "avg_sugars_g",
        "avg_proteins_g",
        "avg_fiber_g",
        "overall_health_rank",
    ).limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 70)
print("GOLD NUTRITION ANALYTICS SUMMARY")
print("=" * 70)
print(f"gold_nutrition_by_category:  {cat_count:>6} categories (min {min_per_category} products)")
print(f"gold_brand_scorecard:        {brand_count:>6} brands (min {min_per_brand} products)")
print(f"gold_category_comparison:    {comparison_count:>6} categories with health ranks")
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
# MAGIC > enforce minimum sample sizes from config to prevent misleading stats from tiny categories."
