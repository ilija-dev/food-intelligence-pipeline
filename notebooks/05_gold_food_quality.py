# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold: Food Quality Scores
# MAGIC
# MAGIC **Purpose**: Analyze how food quality scores (Nutri-Score, NOVA) vary by country.
# MAGIC
# MAGIC Three Gold tables:
# MAGIC 1. **`gold_country_nutriscore`**: Distribution of Nutri-Score grades (A-E) per country
# MAGIC 2. **`gold_ultra_processing_by_country`**: % of NOVA 4 (ultra-processed) products per country
# MAGIC 3. **`gold_nutriscore_vs_nova`**: Cross-tabulation — do ultra-processed foods always score poorly?
# MAGIC
# MAGIC These answer questions like: "Does France have healthier food than the US?"
# MAGIC and "Can a product be ultra-processed (NOVA 4) but still score Nutri-Score A?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Create measures and metrics" — KPI aggregations  
# MAGIC **DP-700 Mapping**: "Build consumption layer" — analytical Gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import yaml
import os
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

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
min_per_country = config["quality"]["min_products_per_country"]  # 100

# Set the active catalog and schema for this notebook
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Read Silver as a Unity Catalog managed table
df_silver = spark.table(f"{catalog}.{schema}.{silver_table}")
print(f"Silver input: {df_silver.count():,} rows")

# Get available columns for dynamic handling
silver_columns = df_silver.columns
has_nutriscore = "nutriscore_grade" in silver_columns
has_nova = "nova_group" in silver_columns

print(f"Available columns: nutriscore_grade={has_nutriscore}, nova_group={has_nova}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold Table: Country Nutri-Score Distribution
# MAGIC
# MAGIC For each country, what percentage of products fall into each Nutri-Score grade?
# MAGIC This reveals national food quality patterns — e.g., France (where Nutri-Score was invented)
# MAGIC may have more products with A/B grades because manufacturers reformulated.

# COMMAND ----------

# Only process if nutriscore_grade exists
if has_nutriscore:
    # Only products that HAVE a Nutri-Score and a country
    df_scored = df_silver.filter(
        F.col("nutriscore_grade").isNotNull() & F.col("primary_country").isNotNull()
    )

    # Count per country per grade
    df_country_grade = (
        df_scored
        .groupBy("primary_country", "nutriscore_grade")
        .agg(F.count("*").alias("product_count"))
    )

    # Get total per country for percentage calculation
    df_country_total = (
        df_scored
        .groupBy("primary_country")
        .agg(F.count("*").alias("total_products"))
        .filter(F.col("total_products") >= min_per_country)
    )

    # Join and calculate percentages
    df_country_nutriscore = (
        df_country_grade
        .join(df_country_total, "primary_country", "inner")  # inner join filters to qualifying countries
        .withColumn(
            "pct_of_country",
            F.round((F.col("product_count") / F.col("total_products")) * 100, 2),
        )
        .orderBy("primary_country", "nutriscore_grade")
    )

    country_ns_count = df_country_nutriscore.select("primary_country").distinct().count()
    print(f"Countries with >= {min_per_country} scored products: {country_ns_count}")

    # Write as Unity Catalog managed table
    gold_country_ns = gold_tables["country_nutriscore"]
    df_country_nutriscore.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{schema}.{gold_country_ns}")

    print(f"Written: {catalog}.{schema}.{gold_country_ns}")

    # Preview: top countries, show grade distribution
    display(
        df_country_nutriscore
        .filter(F.col("primary_country").isin("France", "United States", "Germany", "United Kingdom", "Spain"))
        .orderBy("primary_country", "nutriscore_grade")
    )
else:
    # Create placeholder table
    print("WARNING: nutriscore_grade column not found in Silver table")
    print("Creating placeholder table with message")

    df_country_nutriscore = spark.createDataFrame(
        [("N/A", "N/A", 0, 0, 0.0, "nutriscore_grade column not available in source data")],
        ["primary_country", "nutriscore_grade", "product_count", "total_products", "pct_of_country", "note"]
    )

    country_ns_count = 0

    gold_country_ns = gold_tables["country_nutriscore"]
    df_country_nutriscore.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{schema}.{gold_country_ns}")

    print(f"Written placeholder: {catalog}.{schema}.{gold_country_ns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Table: Ultra-Processing by Country
# MAGIC
# MAGIC What percentage of each country's food products are ultra-processed (NOVA 4)?
# MAGIC This is a key metric for health regulators — NOVA 4 products are associated with
# MAGIC increased health risks in epidemiological studies.
# MAGIC
# MAGIC We also include the full NOVA distribution (1-4) for context.

# COMMAND ----------

# Only process if nova_group exists
if has_nova:
    # Products with NOVA group and country
    df_nova = df_silver.filter(
        F.col("nova_group").isNotNull() & F.col("primary_country").isNotNull()
    )

    # Full NOVA distribution by country
    df_country_nova = (
        df_nova
        .groupBy("primary_country", "nova_group")
        .agg(F.count("*").alias("product_count"))
    )

    df_nova_totals = (
        df_nova
        .groupBy("primary_country")
        .agg(F.count("*").alias("total_products"))
        .filter(F.col("total_products") >= min_per_country)
    )

    # Build the ultra-processing summary
    df_ultra_processing = (
        df_nova_totals
        .join(
            df_nova
            .groupBy("primary_country")
            .agg(
                # NOVA group distribution as percentages
                F.round(F.avg(F.when(F.col("nova_group") == 1, 1).otherwise(0)) * 100, 2)
                .alias("pct_nova_1_unprocessed"),

                F.round(F.avg(F.when(F.col("nova_group") == 2, 1).otherwise(0)) * 100, 2)
                .alias("pct_nova_2_processed_ingredients"),

                F.round(F.avg(F.when(F.col("nova_group") == 3, 1).otherwise(0)) * 100, 2)
                .alias("pct_nova_3_processed"),

                F.round(F.avg(F.when(F.col("nova_group") == 4, 1).otherwise(0)) * 100, 2)
                .alias("pct_nova_4_ultra_processed"),

                # Average NOVA group (higher = more processed food supply)
                F.round(F.avg(F.col("nova_group").cast("double")), 2)
                .alias("avg_nova_group"),
            ),
            "primary_country",
            "inner",
        )
        .orderBy(F.col("pct_nova_4_ultra_processed").desc())
    )

    ultra_count = df_ultra_processing.count()
    print(f"Countries with >= {min_per_country} NOVA-classified products: {ultra_count}")

    # Write as Unity Catalog managed table
    gold_ultra = gold_tables["ultra_processing_by_country"]
    df_ultra_processing.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{schema}.{gold_ultra}")

    print(f"Written: {catalog}.{schema}.{gold_ultra}")
    display(df_ultra_processing.limit(15))
else:
    # Create placeholder table
    print("WARNING: nova_group column not found in Silver table")
    print("Creating placeholder table with message")

    df_ultra_processing = spark.createDataFrame(
        [("N/A", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "nova_group column not available in source data")],
        ["primary_country", "total_products", "pct_nova_1_unprocessed", "pct_nova_2_processed_ingredients",
         "pct_nova_3_processed", "pct_nova_4_ultra_processed", "avg_nova_group", "note"]
    )

    ultra_count = 0

    gold_ultra = gold_tables["ultra_processing_by_country"]
    df_ultra_processing.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{schema}.{gold_ultra}")

    print(f"Written placeholder: {catalog}.{schema}.{gold_ultra}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Table: Nutri-Score vs NOVA Cross-Tabulation
# MAGIC
# MAGIC The interesting question: **Do ultra-processed foods always have bad Nutri-Scores?**
# MAGIC
# MAGIC Spoiler: Not always. A diet soda might be NOVA 4 (ultra-processed, full of additives)
# MAGIC but Nutri-Score A (low calories, no sugar). This table quantifies that tension.

# COMMAND ----------

# Only create cross-tab if both columns exist
if has_nutriscore and has_nova:
    df_cross = (
        df_silver
        .filter(
            F.col("nutriscore_grade").isNotNull() & F.col("nova_group").isNotNull()
        )
        .groupBy("nova_group", "nutriscore_grade")
        .agg(F.count("*").alias("product_count"))
    )

    # Calculate percentages within each NOVA group
    from pyspark.sql import Window

    nova_window = Window.partitionBy("nova_group")
    df_nutriscore_vs_nova = (
        df_cross
        .withColumn("nova_total", F.sum("product_count").over(nova_window))
        .withColumn(
            "pct_within_nova",
            F.round((F.col("product_count") / F.col("nova_total")) * 100, 2),
        )
        .orderBy("nova_group", "nutriscore_grade")
    )

    # Write as Unity Catalog managed table
    gold_ns_nova = gold_tables["nutriscore_vs_nova"]
    df_nutriscore_vs_nova.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{schema}.{gold_ns_nova}")

    print(f"Written: {catalog}.{schema}.{gold_ns_nova}")
    print("\nNutri-Score distribution WITHIN each NOVA group:")
    display(df_nutriscore_vs_nova)
else:
    # Create placeholder explaining which columns are missing
    missing_cols = []
    if not has_nutriscore:
        missing_cols.append("nutriscore_grade")
    if not has_nova:
        missing_cols.append("nova_group")

    missing_msg = f"Missing columns: {', '.join(missing_cols)}. Both are required for cross-tabulation."
    print(f"WARNING: {missing_msg}")
    print("Creating placeholder table with message")

    df_nutriscore_vs_nova = spark.createDataFrame(
        [("N/A", "N/A", 0, 0, 0.0, missing_msg)],
        ["nova_group", "nutriscore_grade", "product_count", "nova_total", "pct_within_nova", "note"]
    )

    gold_ns_nova = gold_tables["nutriscore_vs_nova"]
    df_nutriscore_vs_nova.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{schema}.{gold_ns_nova}")

    print(f"Written placeholder: {catalog}.{schema}.{gold_ns_nova}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

print("=" * 70)
print("GOLD FOOD QUALITY SUMMARY")
print("=" * 70)
print(f"gold_country_nutriscore:           {country_ns_count:>5} countries")
print(f"gold_ultra_processing_by_country:  {ultra_count:>5} countries")
print(f"gold_nutriscore_vs_nova:           {df_nutriscore_vs_nova.count():>5} cells (5 grades x 4 NOVA)")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "The most interesting Gold table is `nutriscore_vs_nova` — a cross-tabulation that reveals
# MAGIC > a real tension in food classification. A diet soda can be NOVA 4 (ultra-processed, full of
# MAGIC > additives) but Nutri-Score A (zero calories). This isn't a data bug — it's a genuine
# MAGIC > policy debate about how to measure 'healthy.' Building this table lets regulators quantify
# MAGIC > exactly how many products fall into each quadrant."
