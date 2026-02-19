# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Gold: Allergen Analysis
# MAGIC
# MAGIC **Purpose**: Analyze allergen prevalence across food categories and countries.
# MAGIC
# MAGIC Two Gold tables:
# MAGIC 1. **`gold_allergen_prevalence`**: Which allergens appear most, broken down by category and country
# MAGIC 2. **`gold_allergen_free_options`**: What % of products in each category are free from each allergen
# MAGIC
# MAGIC These answer questions like: "What percentage of breakfast cereals contain gluten?"
# MAGIC and "Which country has the most dairy-free options in the snacks category?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Create measures and metrics" — business KPIs from array data  
# MAGIC **DP-700 Mapping**: "Build consumption layer" — exploding arrays for analytical tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import yaml
import os
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

# Unity Catalog identifiers
catalog = config["catalog"]
schema = config["schema"]
silver_table = config["tables"]["silver"]["products"]
gold_tables = config["tables"]["gold"]
min_per_category = config["quality"]["min_products_per_category"]

# Set default catalog and schema context
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Read Silver as a Unity Catalog managed table
df_silver = spark.table(f"{catalog}.{schema}.{silver_table}")
print(f"Silver input: {df_silver.count():,} rows")

# Check if allergens_tags exists in the data
if "allergens_tags" not in df_silver.columns:
    print("\n" + "=" * 70)
    print("WARNING: allergens_tags column not found in Silver data")
    print("=" * 70)
    print("Allergen analysis requires the allergens_tags column to be present.")
    print("Creating placeholder tables so downstream processes don't fail.")
    print("=" * 70 + "\n")

    # Create placeholder table 1: allergen_prevalence
    placeholder_prevalence = spark.createDataFrame(
        [("No allergen data available", "N/A", 0, 0, 0.0)],
        ["primary_category", "allergen", "products_with_allergen", "total_products_in_category", "prevalence_pct"]
    )
    prevalence_table = gold_tables["allergen_prevalence"]
    (
        placeholder_prevalence.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{prevalence_table}")
    )
    print(f"Created placeholder: {catalog}.{schema}.{prevalence_table}")

    # Create placeholder table 2: allergen_free_options
    placeholder_free = spark.createDataFrame(
        [("No allergen data available", "N/A", 0, 0, 0.0)],
        ["primary_category", "allergen", "total_with_allergen_data", "free_from_count", "pct_free_from"]
    )
    free_table = gold_tables["allergen_free_options"]
    (
        placeholder_free.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{free_table}")
    )
    print(f"Created placeholder: {catalog}.{schema}.{free_table}")

    print("\n" + "=" * 70)
    print("GOLD ALLERGEN ANALYSIS SUMMARY (SKIPPED - NO DATA)")
    print("=" * 70)
    print("Allergen analysis skipped: allergens_tags column not available")
    print("=" * 70)

    # Exit early
    dbutils.notebook.exit("Skipped: allergens_tags column not available")

# Defensive check for primary_category
if "primary_category" not in df_silver.columns:
    print("\nWARNING: primary_category column not found. Creating placeholder tables.")
    # Create minimal schema without primary_category grouping
    use_primary_category = False
else:
    use_primary_category = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse Allergen Tags
# MAGIC
# MAGIC `allergens_tags` is a comma-separated string or array like `"en:gluten,en:milk,en:eggs"`.
# MAGIC We need to:
# MAGIC 1. Explode the array so each allergen gets its own row
# MAGIC 2. Clean the tag: `"en:milk"` → `"Milk"`
# MAGIC
# MAGIC This is a classic **one-to-many explosion** pattern — one product row becomes N allergen rows.

# COMMAND ----------

# UDF to clean allergen tags
@F.udf(StringType())
def clean_allergen_tag(tag):
    """Clean allergen tag: 'en:milk' → 'Milk'"""
    if tag is None:
        return None
    tag = tag.strip()
    if ":" in tag:
        tag = tag.split(":", 1)[1]
    return tag.replace("-", " ").strip().title()

# Handle allergens_tags as either string or array
# Check the data type to determine how to filter
df_schema = {field.name: str(field.dataType) for field in df_silver.schema.fields}
allergens_type = df_schema.get("allergens_tags", "").lower()

if "array" in allergens_type:
    # Array type: filter non-null and non-empty
    df_with_allergens = df_silver.filter(
        F.col("allergens_tags").isNotNull() & (F.size(F.col("allergens_tags")) > 0)
    )
else:
    # String type (or other): filter non-null and not empty string
    df_with_allergens = df_silver.filter(
        F.col("allergens_tags").isNotNull() & (F.col("allergens_tags") != "")
    )

# Try to split — if it's already an array, this is a no-op via coalesce logic
df_exploded = (
    df_with_allergens
    .withColumn(
        "allergen_array",
        F.when(
            F.col("allergens_tags").cast(StringType()).contains(","),
            F.split(F.col("allergens_tags"), ","),
        ).otherwise(F.array(F.col("allergens_tags").cast(StringType()))),
    )
    .withColumn("allergen_raw", F.explode("allergen_array"))
    .withColumn("allergen", clean_allergen_tag(F.col("allergen_raw")))
    .filter(F.col("allergen").isNotNull() & (F.col("allergen") != ""))
)

allergen_rows = df_exploded.count()
unique_allergens = df_exploded.select("allergen").distinct().count() if allergen_rows > 0 else 0
print(f"Exploded allergen rows: {allergen_rows:,}")
print(f"Unique allergens: {unique_allergens}")

# Show top allergens (only if data exists)
if allergen_rows > 0:
    print("\nTop 15 allergens overall:")
    display(
        df_exploded
        .groupBy("allergen")
        .agg(F.countDistinct("code").alias("product_count"))
        .orderBy(F.col("product_count").desc())
        .limit(15)
    )
else:
    print("\nWarning: No allergen data found in products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Table: Allergen Prevalence
# MAGIC
# MAGIC For each allergen + category combination:
# MAGIC - How many products contain this allergen?
# MAGIC - What percentage of the category does that represent?
# MAGIC
# MAGIC Also broken down by country for regional analysis.

# COMMAND ----------

# Allergen prevalence by category - defensive grouping
if use_primary_category:
    df_allergen_by_cat = (
        df_exploded
        .filter(F.col("primary_category").isNotNull())
        .groupBy("primary_category", "allergen")
        .agg(F.countDistinct("code").alias("products_with_allergen"))
    )

    # Total products per category (from all Silver, not just those with allergens)
    df_cat_totals = (
        df_silver
        .filter(F.col("primary_category").isNotNull())
        .groupBy("primary_category")
        .agg(F.countDistinct("code").alias("total_products_in_category"))
        .filter(F.col("total_products_in_category") >= min_per_category)
    )
else:
    # Fallback: aggregate at global level without primary_category
    df_allergen_by_cat = (
        df_exploded
        .groupBy("allergen")
        .agg(F.countDistinct("code").alias("products_with_allergen"))
        .withColumn("primary_category", F.lit("All Products"))
    )

    # Total products without category breakdown
    total_count = df_silver.select(F.countDistinct("code")).collect()[0][0]
    df_cat_totals = spark.createDataFrame(
        [("All Products", total_count)],
        ["primary_category", "total_products_in_category"]
    )

df_allergen_prevalence = (
    df_allergen_by_cat
    .join(df_cat_totals, "primary_category", "inner")
    .withColumn(
        "prevalence_pct",
        F.round(
            (F.col("products_with_allergen") / F.col("total_products_in_category")) * 100,
            2,
        ),
    )
    .orderBy("primary_category", F.col("prevalence_pct").desc())
)

prevalence_count = df_allergen_prevalence.count()
print(f"Allergen-category combinations: {prevalence_count:,}")

# Handle empty results gracefully
if prevalence_count == 0:
    print("Warning: No allergen prevalence data available")
    # Create empty DataFrame with correct schema
    df_allergen_prevalence = spark.createDataFrame(
        [],
        "primary_category STRING, allergen STRING, products_with_allergen LONG, total_products_in_category LONG, prevalence_pct DOUBLE"
    )

# Write to Unity Catalog as a managed table
prevalence_table = gold_tables["allergen_prevalence"]
(
    df_allergen_prevalence.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{prevalence_table}")
)

print(f"Written: {catalog}.{schema}.{prevalence_table}")

# Show: what allergens dominate in common categories? (only if data exists)
if prevalence_count > 0:
    display(
        df_allergen_prevalence
        .filter(F.col("prevalence_pct") > 10)
        .orderBy(F.col("prevalence_pct").desc())
        .limit(20)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Table: Allergen-Free Options
# MAGIC
# MAGIC For each category, what percentage of products are FREE FROM each major allergen?
# MAGIC
# MAGIC This is the inverse view — useful for consumers with dietary restrictions.
# MAGIC "I'm allergic to gluten. What percentage of breakfast cereals are gluten-free?"

# COMMAND ----------

# Major allergens to track
major_allergens = [
    "Gluten", "Milk", "Eggs", "Nuts", "Peanuts",
    "Soybeans", "Fish", "Sesame Seeds", "Celery", "Mustard",
]

# For each category, count how many products do NOT contain each allergen
# This requires knowing which products have allergen data at all

# Products with allergen data (either they list allergens or they're flagged as allergen-free)
# Defensive: check if traces_tags exists
df_with_allergen_data = df_silver.filter(
    F.col("allergens_tags").isNotNull() | 
    (F.col("traces_tags").isNotNull() if "traces_tags" in df_silver.columns else F.lit(False))
)

# For each major allergen, check if it's in the allergens_tags
allergen_free_dfs = []

for allergen in major_allergens:
    allergen_lower = allergen.lower().replace(" ", "-")

    if use_primary_category:
        df_free = (
            df_with_allergen_data
            .filter(F.col("primary_category").isNotNull())
            .withColumn(
                "contains_allergen",
                F.when(
                    F.lower(F.col("allergens_tags").cast(StringType())).contains(allergen_lower),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            .groupBy("primary_category")
            .agg(
                F.count("*").alias("total_with_allergen_data"),
                F.sum(F.when(~F.col("contains_allergen"), 1).otherwise(0)).alias("free_from_count"),
            )
            .filter(F.col("total_with_allergen_data") >= min_per_category)
            .withColumn("allergen", F.lit(allergen))
            .withColumn(
                "pct_free_from",
                F.round(
                    (F.col("free_from_count") / F.col("total_with_allergen_data")) * 100, 2
                ),
            )
            .select(
                "primary_category",
                "allergen",
                "total_with_allergen_data",
                "free_from_count",
                "pct_free_from",
            )
        )
    else:
        # Fallback: aggregate at global level
        df_free = (
            df_with_allergen_data
            .withColumn(
                "contains_allergen",
                F.when(
                    F.lower(F.col("allergens_tags").cast(StringType())).contains(allergen_lower),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            .agg(
                F.count("*").alias("total_with_allergen_data"),
                F.sum(F.when(~F.col("contains_allergen"), 1).otherwise(0)).alias("free_from_count"),
            )
            .withColumn("allergen", F.lit(allergen))
            .withColumn("primary_category", F.lit("All Products"))
            .withColumn(
                "pct_free_from",
                F.round(
                    (F.col("free_from_count") / F.col("total_with_allergen_data")) * 100, 2
                ),
            )
            .select(
                "primary_category",
                "allergen",
                "total_with_allergen_data",
                "free_from_count",
                "pct_free_from",
            )
        )
    allergen_free_dfs.append(df_free)

# Union all allergen DataFrames
from functools import reduce

if allergen_free_dfs:
    df_allergen_free = reduce(lambda a, b: a.unionByName(b), allergen_free_dfs)
    df_allergen_free = df_allergen_free.orderBy("primary_category", "allergen")
    free_count = df_allergen_free.count()
    print(f"Category-allergen free-from combinations: {free_count:,}")
else:
    # Create empty DataFrame with correct schema if no data
    df_allergen_free = spark.createDataFrame(
        [],
        "primary_category STRING, allergen STRING, total_with_allergen_data LONG, free_from_count LONG, pct_free_from DOUBLE"
    )
    free_count = 0
    print("Warning: No allergen-free data available")

# Write to Unity Catalog as a managed table
free_table = gold_tables["allergen_free_options"]
(
    df_allergen_free.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{free_table}")
)

print(f"Written: {catalog}.{schema}.{free_table}")

# Show: which categories have the most gluten-free options? (only if data exists)
if free_count > 0:
    print("\nCategories with highest % gluten-free products:")
    display(
        df_allergen_free
        .filter(F.col("allergen") == "Gluten")
        .orderBy(F.col("pct_free_from").desc())
        .limit(15)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

print("=" * 70)
print("GOLD ALLERGEN ANALYSIS SUMMARY")
print("=" * 70)
print(f"Unique allergens found:            {unique_allergens:>6}")
print(f"{prevalence_table}:  {prevalence_count:>6} category-allergen pairs")
print(f"{free_table}:  {free_count:>6} category-allergen pairs")
print(f"Major allergens tracked:           {len(major_allergens):>6}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "The allergen analysis uses `explode()` to handle one-to-many relationships — one product
# MAGIC > can contain multiple allergens, so I explode the array and aggregate at the allergen level.
# MAGIC > I built two views: prevalence (how common is gluten in cereals?) and free-from (what % of
# MAGIC > cereals are gluten-free?). The free-from table required iterating over 10 major allergens
# MAGIC > and union-ing the results — I used `functools.reduce` to combine them cleanly instead of
# MAGIC > writing 10 separate queries."
