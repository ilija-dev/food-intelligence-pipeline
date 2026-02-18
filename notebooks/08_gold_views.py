# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Gold: SQL Views
# MAGIC
# MAGIC **Purpose**: Create SQL views on top of Silver and Gold tables for flexible querying.
# MAGIC
# MAGIC Not every analytical question needs its own materialized Gold table. SQL views let analysts
# MAGIC write ad-hoc queries against clean Silver data without materializing every possible cut.
# MAGIC
# MAGIC Views are:
# MAGIC - **Zero storage cost** — they're just saved queries, not physical tables
# MAGIC - **Always fresh** — they read from the underlying table at query time
# MAGIC - **Composable** — analysts can join views or add their own filters
# MAGIC
# MAGIC We materialize (Gold tables) when the query is expensive and runs frequently.
# MAGIC We use views when the query is ad-hoc or the underlying data changes often.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DP-600 Mapping**: "Create semantic models" — views as a semantic layer  
# MAGIC **DP-700 Mapping**: "Optimize query patterns" — materialized tables vs views

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

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

# Unity Catalog: three-level namespace (catalog.schema.table)
catalog = config["catalog"]
schema = config["schema"]
full_schema = f"{catalog}.{schema}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Using Unity Catalog namespace: {full_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. View: High-Quality Products
# MAGIC
# MAGIC A filtered view of Silver showing only products with good data quality.
# MAGIC Analysts can query this when they want reliable data without worrying about
# MAGIC completeness filtering themselves.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {full_schema}.vw_high_quality_products AS
    SELECT *
    FROM {full_schema}.silver_products
    WHERE data_quality_score >= 0.7
      AND nutrition_completeness >= 0.5
      AND primary_category IS NOT NULL
      AND primary_country IS NOT NULL
""")

hq_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_schema}.vw_high_quality_products").collect()[0]["cnt"]
print(f"vw_high_quality_products: {hq_count:,} rows (data_quality >= 0.7, nutrition >= 50%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. View: Product Search
# MAGIC
# MAGIC A simplified view with the most commonly searched fields.
# MAGIC Think of this as the "product card" view — what you'd show in a search result.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {full_schema}.vw_product_search AS
    SELECT
        code,
        product_name,
        brands,
        primary_category,
        primary_country,
        nutriscore_grade,
        nova_group,
        health_tier,
        energy_kcal_100g,
        sugars_100g,
        proteins_100g,
        fiber_100g,
        is_ultra_processed,
        data_quality_score
    FROM {full_schema}.silver_products
    WHERE product_name IS NOT NULL
      AND code IS NOT NULL
""")

search_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_schema}.vw_product_search").collect()[0]["cnt"]
print(f"vw_product_search: {search_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. View: Country Health Overview
# MAGIC
# MAGIC Combines data from multiple Gold tables into a single country-level summary.
# MAGIC This is what a dashboard "Country Health" page would query.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {full_schema}.vw_country_health_overview AS
    SELECT
        u.primary_country,
        u.total_products,
        u.avg_nova_group,
        u.pct_nova_4_ultra_processed,
        u.pct_nova_1_unprocessed,

        -- Aggregate Nutri-Score distribution from the detail table
        ns_a.pct_a,
        ns_e.pct_e

    FROM {full_schema}.gold_ultra_processing_by_country u

    LEFT JOIN (
        SELECT primary_country, pct_of_country AS pct_a
        FROM {full_schema}.gold_country_nutriscore
        WHERE nutriscore_grade = 'a'
    ) ns_a ON u.primary_country = ns_a.primary_country

    LEFT JOIN (
        SELECT primary_country, pct_of_country AS pct_e
        FROM {full_schema}.gold_country_nutriscore
        WHERE nutriscore_grade = 'e'
    ) ns_e ON u.primary_country = ns_e.primary_country

    ORDER BY u.total_products DESC
""")

overview_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_schema}.vw_country_health_overview").collect()[0]["cnt"]
print(f"vw_country_health_overview: {overview_count:,} countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. View: Category Nutrition Quick Look
# MAGIC
# MAGIC Simplified nutrition-by-category view with health interpretation columns
# MAGIC for non-technical dashboard consumers.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {full_schema}.vw_category_nutrition AS
    SELECT
        primary_category,
        product_count,
        avg_energy_kcal,
        avg_sugars_g,
        avg_fat_g,
        avg_proteins_g,
        avg_fiber_g,
        avg_salt_g,

        -- Interpretive columns for dashboards
        CASE
            WHEN avg_sugars_g > 20 THEN 'High Sugar'
            WHEN avg_sugars_g > 10 THEN 'Moderate Sugar'
            ELSE 'Low Sugar'
        END AS sugar_level,

        CASE
            WHEN avg_energy_kcal > 400 THEN 'High Calorie'
            WHEN avg_energy_kcal > 200 THEN 'Moderate Calorie'
            ELSE 'Low Calorie'
        END AS calorie_level,

        CASE
            WHEN avg_proteins_g > 15 THEN 'High Protein'
            WHEN avg_proteins_g > 5 THEN 'Moderate Protein'
            ELSE 'Low Protein'
        END AS protein_level,

        avg_nutrition_completeness,
        overall_health_rank

    FROM {full_schema}.gold_category_comparison
    ORDER BY overall_health_rank
""")

cat_view_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_schema}.vw_category_nutrition").collect()[0]["cnt"]
print(f"vw_category_nutrition: {cat_view_count:,} categories with interpretive labels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. View: Brand Leaderboard
# MAGIC
# MAGIC Top brands ranked by average Nutri-Score, for competitive analysis dashboards.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {full_schema}.vw_brand_leaderboard AS
    SELECT
        brands,
        product_count,
        avg_nutriscore_grade,
        avg_nutriscore_numeric,
        avg_nova_group,
        pct_healthy,
        pct_unhealthy,
        pct_ultra_processed,
        avg_energy_kcal,
        avg_sugars_g,
        avg_proteins_g,

        -- Rank brands by health score (lower nutriscore_numeric = healthier)
        DENSE_RANK() OVER (ORDER BY avg_nutriscore_numeric ASC) AS health_rank

    FROM {full_schema}.gold_brand_scorecard
    WHERE avg_nutriscore_numeric IS NOT NULL
    ORDER BY health_rank
""")

brand_view_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_schema}.vw_brand_leaderboard").collect()[0]["cnt"]
print(f"vw_brand_leaderboard: {brand_view_count:,} brands ranked by health score")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 70)
print("GOLD SQL VIEWS SUMMARY")
print("=" * 70)

views = [
    ("vw_high_quality_products", "Filtered Silver: quality >= 0.7"),
    ("vw_product_search", "Simplified product search fields"),
    ("vw_country_health_overview", "Country-level health composite"),
    ("vw_category_nutrition", "Category nutrition with interpretive labels"),
    ("vw_brand_leaderboard", "Brands ranked by health score"),
]

for view_name, description in views:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_schema}.{view_name}").collect()[0]["cnt"]
    print(f"  {view_name:<35} {cnt:>8,} rows  | {description}")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Interview Talking Point
# MAGIC
# MAGIC > "I use a mix of materialized Gold tables and SQL views. Expensive aggregations that
# MAGIC > dashboards query repeatedly — like brand scorecards — are materialized as Delta tables
# MAGIC > for fast reads. Ad-hoc or composable queries — like the country health overview that
# MAGIC > joins multiple Gold tables — are views with zero storage cost. The `vw_high_quality_products`
# MAGIC > view is particularly useful: it pre-filters to records with quality score >= 0.7 so analysts
# MAGIC > don't have to remember the threshold. This maps to the semantic layer concept in DP-600.
# MAGIC > All views are registered in Unity Catalog under `main.food_intelligence`, giving us
# MAGIC > centralized governance — access control, lineage tracking, and discoverability in a single
# MAGIC > three-level namespace."
