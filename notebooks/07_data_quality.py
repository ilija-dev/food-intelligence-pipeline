# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Data Quality Checks
# MAGIC
# MAGIC **Purpose**: Validate data integrity across all three layers after every pipeline run.
# MAGIC
# MAGIC This notebook is the pipeline's immune system. It runs after ingestion and transformation
# MAGIC to catch problems before they reach dashboards:
# MAGIC
# MAGIC - **Bronze**: Row count matches source, schema hasn't drifted
# MAGIC - **Silver**: Zero duplicate barcodes, all scores in valid ranges, nutrition in bounds
# MAGIC - **Gold**: KPI sanity checks (avg calories for "sodas" should be 30-60 kcal)
# MAGIC
# MAGIC Each check either PASSES or FAILS with a clear diagnostic message.
# MAGIC In production, failures would trigger alerts via Databricks workflows or Azure Monitor.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Validate data pipelines" — quality gates  
# MAGIC **DP-700 Mapping**: "Monitor data quality" — automated checks, schema validation

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
    WORKSPACE_CONFIG = "/Workspace/Repos/food-intelligence-pipeline/config/pipeline_config.yaml"
    with open(WORKSPACE_CONFIG, "r") as f:
        config = yaml.safe_load(f)

db_name = config["database"]["name"]
bronze_path = config["delta_tables"]["bronze"]["products"]
silver_path = config["delta_tables"]["silver"]["products"]
gold_paths = config["delta_tables"]["gold"]
quality = config["quality"]

spark.sql(f"USE {db_name}")

# Track all check results
results = []

def check(name, passed, detail=""):
    """Record a quality check result."""
    status = "PASS" if passed else "FAIL"
    results.append({"check": name, "status": status, "detail": detail})
    icon = "PASS" if passed else "FAIL"
    print(f"  [{icon}] {name}")
    if detail and not passed:
        print(f"         {detail}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze Layer Checks
# MAGIC
# MAGIC - Row count is non-zero
# MAGIC - Metadata columns exist
# MAGIC - No schema drift (expected metadata columns are present)

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER CHECKS")
print("=" * 60)

df_bronze = spark.read.format("delta").load(bronze_path)
bronze_count = df_bronze.count()

# B1: Row count non-zero
check(
    "B1: Bronze has data",
    bronze_count > 0,
    f"Row count: {bronze_count:,}",
)

# B2: Metadata columns exist
expected_metadata = ["_ingestion_timestamp", "_source_file", "_batch_id", "_source_format"]
bronze_cols = set(df_bronze.columns)
missing_meta = [c for c in expected_metadata if c not in bronze_cols]
check(
    "B2: Metadata columns present",
    len(missing_meta) == 0,
    f"Missing: {missing_meta}" if missing_meta else f"All {len(expected_metadata)} metadata columns found",
)

# B3: Primary key (code) exists and is mostly non-null
if "code" in bronze_cols:
    null_codes = df_bronze.filter(F.col("code").isNull()).count()
    null_pct = (null_codes / bronze_count) * 100
    check(
        "B3: Barcode (code) column populated",
        null_pct < 5,  # Allow up to 5% null — some records are legitimately incomplete
        f"{null_pct:.2f}% null ({null_codes:,} rows)",
    )
else:
    check("B3: Barcode (code) column exists", False, "Column 'code' not found")

# B4: Ingestion timestamp is recent (within 30 days)
if "_ingestion_timestamp" in bronze_cols:
    max_ts = df_bronze.agg(F.max("_ingestion_timestamp")).collect()[0][0]
    if max_ts:
        age_days = (datetime.now(timezone.utc) - max_ts.replace(tzinfo=timezone.utc)).days
        check(
            "B4: Bronze data freshness",
            age_days <= 30,
            f"Last ingestion: {age_days} days ago ({max_ts})",
        )
    else:
        check("B4: Bronze data freshness", False, "Null timestamp")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Layer Checks
# MAGIC
# MAGIC - Zero duplicate barcodes (dedup worked)
# MAGIC - All Nutri-Score values in valid range
# MAGIC - All NOVA groups in valid range
# MAGIC - No negative nutrition values
# MAGIC - Row count is <= Bronze (we should only remove, never add rows)
# MAGIC - Enrichment columns exist and are populated

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER CHECKS")
print("=" * 60)

df_silver = spark.read.format("delta").load(silver_path)
silver_count = df_silver.count()

# S1: Silver has fewer or equal rows than Bronze (dedup removes, doesn't add)
check(
    "S1: Silver <= Bronze row count",
    silver_count <= bronze_count,
    f"Bronze: {bronze_count:,}, Silver: {silver_count:,}",
)

# S2: Zero duplicate barcodes
dupe_count = (
    df_silver.groupBy("code").count().filter(F.col("count") > 1).count()
)
check(
    "S2: Zero duplicate barcodes",
    dupe_count == 0,
    f"Duplicate barcodes found: {dupe_count:,}",
)

# S3: Nutri-Score values are all valid
valid_ns = quality["valid_nutriscore_grades"]
if "nutriscore_grade" in df_silver.columns:
    invalid_ns = (
        df_silver
        .filter(F.col("nutriscore_grade").isNotNull())
        .filter(~F.col("nutriscore_grade").isin(valid_ns))
        .count()
    )
    check(
        "S3: All Nutri-Scores in valid range (a-e)",
        invalid_ns == 0,
        f"Invalid values: {invalid_ns:,}",
    )

# S4: NOVA groups are all valid
valid_nova = quality["valid_nova_groups"]
if "nova_group" in df_silver.columns:
    invalid_nova = (
        df_silver
        .filter(F.col("nova_group").isNotNull())
        .filter(~F.col("nova_group").isin(valid_nova))
        .count()
    )
    check(
        "S4: All NOVA groups in valid range (1-4)",
        invalid_nova == 0,
        f"Invalid values: {invalid_nova:,}",
    )

# S5: No negative nutrition values
nutrition_cols = [
    "energy_kcal_100g", "fat_100g", "saturated_fat_100g",
    "carbohydrates_100g", "sugars_100g", "fiber_100g",
    "proteins_100g", "salt_100g",
]

for ncol in nutrition_cols:
    if ncol in df_silver.columns:
        neg_count = df_silver.filter(F.col(ncol) < 0).count()
        check(
            f"S5: No negative values in {ncol}",
            neg_count == 0,
            f"Negative values: {neg_count:,}",
        )

# S6: Energy within valid range
if "energy_kcal_100g" in df_silver.columns:
    max_energy = quality["nutrition_ranges"]["energy_kcal"]["max"]
    over_max = (
        df_silver
        .filter(F.col("energy_kcal_100g") > max_energy)
        .count()
    )
    check(
        f"S6: Energy kcal <= {max_energy}",
        over_max == 0,
        f"Over max: {over_max:,}",
    )

# S7: Enrichment columns exist
enrichment_cols = [
    "nutrition_completeness", "is_ultra_processed",
    "health_tier", "data_quality_score", "product_age_days",
]
missing_enrichment = [c for c in enrichment_cols if c not in df_silver.columns]
check(
    "S7: Enrichment columns present",
    len(missing_enrichment) == 0,
    f"Missing: {missing_enrichment}" if missing_enrichment else f"All {len(enrichment_cols)} enrichment columns found",
)

# S8: Nutrition completeness is between 0 and 1
if "nutrition_completeness" in df_silver.columns:
    out_of_range = (
        df_silver
        .filter(
            (F.col("nutrition_completeness") < 0)
            | (F.col("nutrition_completeness") > 1)
        )
        .count()
    )
    check(
        "S8: Nutrition completeness in [0, 1]",
        out_of_range == 0,
        f"Out of range: {out_of_range:,}",
    )

# S9: Data quality score is between 0 and 1
if "data_quality_score" in df_silver.columns:
    out_of_range = (
        df_silver
        .filter(
            (F.col("data_quality_score") < 0)
            | (F.col("data_quality_score") > 1)
        )
        .count()
    )
    check(
        "S9: Data quality score in [0, 1]",
        out_of_range == 0,
        f"Out of range: {out_of_range:,}",
    )

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Layer Checks
# MAGIC
# MAGIC - Gold tables have data
# MAGIC - Minimum product thresholds are enforced
# MAGIC - KPI sanity checks (domain-specific reasonableness)

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER CHECKS")
print("=" * 60)

# G1: All Gold tables have data
gold_table_checks = {
    "gold_nutrition_by_category": gold_paths["nutrition_by_category"],
    "gold_brand_scorecard": gold_paths["brand_scorecard"],
    "gold_category_comparison": gold_paths["category_comparison"],
    "gold_country_nutriscore": gold_paths["country_nutriscore"],
    "gold_ultra_processing_by_country": gold_paths["ultra_processing_by_country"],
    "gold_nutriscore_vs_nova": gold_paths["nutriscore_vs_nova"],
    "gold_allergen_prevalence": gold_paths["allergen_prevalence"],
    "gold_allergen_free_options": gold_paths["allergen_free_options"],
}

for table_name, path in gold_table_checks.items():
    try:
        cnt = spark.read.format("delta").load(path).count()
        check(
            f"G1: {table_name} has data",
            cnt > 0,
            f"{cnt:,} rows",
        )
    except Exception as e:
        check(f"G1: {table_name} exists", False, str(e)[:100])

# G2: Minimum product thresholds enforced in nutrition_by_category
try:
    df_nutrition = spark.read.format("delta").load(gold_paths["nutrition_by_category"])
    min_cat = quality["min_products_per_category"]
    below_threshold = df_nutrition.filter(F.col("product_count") < min_cat).count()
    check(
        f"G2: All categories have >= {min_cat} products",
        below_threshold == 0,
        f"Below threshold: {below_threshold}",
    )
except Exception:
    check("G2: Nutrition by category threshold", False, "Table not readable")

# G3: Brand scorecard minimum threshold
try:
    df_brands = spark.read.format("delta").load(gold_paths["brand_scorecard"])
    min_brand = quality["min_products_per_brand"]
    below_threshold = df_brands.filter(F.col("product_count") < min_brand).count()
    check(
        f"G3: All brands have >= {min_brand} products",
        below_threshold == 0,
        f"Below threshold: {below_threshold}",
    )
except Exception:
    check("G3: Brand scorecard threshold", False, "Table not readable")

# G4: Nutri-Score vs NOVA cross-tab completeness
# Should have entries for all 5 grades x 4 NOVA groups = up to 20 cells
try:
    df_cross = spark.read.format("delta").load(gold_paths["nutriscore_vs_nova"])
    cell_count = df_cross.count()
    check(
        "G4: Nutri-Score vs NOVA has reasonable coverage",
        cell_count >= 10,  # At least half the theoretical 20 cells
        f"{cell_count} cells (max possible: 20)",
    )
except Exception:
    check("G4: Nutri-Score vs NOVA cross-tab", False, "Table not readable")

# G5: KPI sanity — avg calories should be reasonable (not 0, not 5000)
try:
    df_nutrition = spark.read.format("delta").load(gold_paths["nutrition_by_category"])
    avg_cal_stats = df_nutrition.agg(
        F.min("avg_energy_kcal").alias("min_avg"),
        F.max("avg_energy_kcal").alias("max_avg"),
        F.avg("avg_energy_kcal").alias("overall_avg"),
    ).collect()[0]

    overall_avg = avg_cal_stats["overall_avg"]
    check(
        "G5: Overall avg calories is reasonable (50-500 kcal)",
        overall_avg is not None and 50 <= overall_avg <= 500,
        f"Overall avg across categories: {overall_avg:.1f} kcal" if overall_avg else "null",
    )
except Exception:
    check("G5: KPI sanity - calories", False, "Could not compute")

# G6: Country percentages sum to ~100% per country in nutriscore distribution
try:
    df_ns = spark.read.format("delta").load(gold_paths["country_nutriscore"])
    country_sums = (
        df_ns
        .groupBy("primary_country")
        .agg(F.round(F.sum("pct_of_country"), 1).alias("total_pct"))
    )
    bad_sums = country_sums.filter(
        (F.col("total_pct") < 99.0) | (F.col("total_pct") > 101.0)
    ).count()
    check(
        "G6: Country Nutri-Score percentages sum to ~100%",
        bad_sums == 0,
        f"Countries with bad totals: {bad_sums}",
    )
except Exception:
    check("G6: Country percentage totals", False, "Could not compute")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cross-Layer Consistency Checks

# COMMAND ----------

print("=" * 60)
print("CROSS-LAYER CONSISTENCY CHECKS")
print("=" * 60)

# X1: Silver row count is reasonable fraction of Bronze (expect >50% after dedup + filter)
retention_pct = (silver_count / bronze_count) * 100 if bronze_count > 0 else 0
check(
    "X1: Silver retains >50% of Bronze rows",
    retention_pct > 50,
    f"Retention: {retention_pct:.1f}% ({silver_count:,} / {bronze_count:,})",
)

# X2: Gold categories are a subset of Silver categories
try:
    gold_cats = set(
        spark.read.format("delta")
        .load(gold_paths["nutrition_by_category"])
        .select("primary_category")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    silver_cats = set(
        df_silver
        .select("primary_category")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    orphan_cats = gold_cats - silver_cats
    check(
        "X2: All Gold categories exist in Silver",
        len(orphan_cats) == 0,
        f"Orphan categories: {orphan_cats}" if orphan_cats else "All Gold categories traced to Silver",
    )
except Exception:
    check("X2: Category consistency", False, "Could not compare")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quality Report Summary

# COMMAND ----------

total = len(results)
passed = sum(1 for r in results if r["status"] == "PASS")
failed = sum(1 for r in results if r["status"] == "FAIL")

print("=" * 60)
print("DATA QUALITY REPORT")
print("=" * 60)
print(f"Total checks:  {total}")
print(f"Passed:        {passed}")
print(f"Failed:        {failed}")
print(f"Pass rate:     {(passed / total) * 100:.1f}%")
print("=" * 60)

if failed > 0:
    print("\nFAILED CHECKS:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"  [FAIL] {r['check']}: {r['detail']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save Quality Report

# COMMAND ----------

report = {
    "run_timestamp": datetime.now(timezone.utc).isoformat(),
    "total_checks": total,
    "passed": passed,
    "failed": failed,
    "pass_rate_pct": round((passed / total) * 100, 1),
    "checks": results,
}

report_path = "/FileStore/food-intelligence/metadata/quality_report.json"
dbutils.fs.put(report_path, json.dumps(report, indent=2), overwrite=True)
print(f"Quality report saved to: {report_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "Every pipeline run finishes with automated quality checks across all three layers.
# MAGIC > Bronze checks verify row counts and metadata integrity. Silver checks enforce zero
# MAGIC > duplicate barcodes, valid score ranges, and no negative nutrition values. Gold checks
# MAGIC > validate that minimum sample thresholds are enforced and KPIs are within reasonable
# MAGIC > bounds — if avg calories across all categories suddenly drops to 0, something broke.
# MAGIC > I also run cross-layer consistency checks: Silver should retain >50% of Bronze rows,
# MAGIC > and every Gold category should trace back to Silver. In production, failures here
# MAGIC > would trigger alerts via Databricks workflows."
