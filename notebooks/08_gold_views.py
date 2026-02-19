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

# Get actual columns from Silver to build dynamic view
df_silver_check = spark.table(f"{full_schema}.silver_products")
silver_cols = df_silver_check.columns

# Build column list based on available columns
search_columns = [
    "code", "product_name", "brands", "primary_category", "primary_country",
    "nutriscore_grade", "nova_group", "health_tier", "is_ultra_processed",
    "data_quality_score"
]
# Add nutrition columns if they exist
nutrition_cols = ["energy_kcal_100g", "sugars_100g", "proteins_100g", "fiber_100g"]
for col in nutrition_cols:
    if col in silver_cols:
        search_columns.append(col)

cols_str = ",\n        ".join(search_columns)

spark.sql(f"""
    CREATE OR REPLACE VIEW {full_schema}.vw_product_search AS
    SELECT
        {cols_str}
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

# Get actual columns from gold_category_comparison to build dynamic view
df_cat_check = spark.table(f"{full_schema}.gold_category_comparison")
cat_cols = df_cat_check.columns

# Build column list based on available columns
base_cat_columns = [
    "primary_category",
    "product_count",
]

# Add nutrition columns if they exist
nutrition_agg_cols = ["avg_energy_kcal", "avg_sugars_g", "avg_fat_g", "avg_proteins_g", "avg_fiber_g", "avg_salt_g"]
available_nutrition_cols = [col for col in nutrition_agg_cols if col in cat_cols]

# Add other metadata columns
metadata_cols = ["avg_nutrition_completeness", "overall_health_rank"]
available_metadata_cols = [col for col in metadata_cols if col in cat_cols]

# Combine all columns
cat_select_columns = base_cat_columns + available_nutrition_cols + available_metadata_cols

cat_cols_str = ",\n        ".join(cat_select_columns)

# Build CASE statements only for columns that exist
case_statements = []

if "avg_sugars_g" in cat_cols:
    case_statements.append("""
        CASE
            WHEN avg_sugars_g > 20 THEN 'High Sugar'
            WHEN avg_sugars_g > 10 THEN 'Moderate Sugar'
            ELSE 'Low Sugar'
        END AS sugar_level""")

if "avg_energy_kcal" in cat_cols:
    case_statements.append("""
        CASE
            WHEN avg_energy_kcal > 400 THEN 'High Calorie'
            WHEN avg_energy_kcal > 200 THEN 'Moderate Calorie'
            ELSE 'Low Calorie'
        END AS calorie_level""")

if "avg_proteins_g" in cat_cols:
    case_statements.append("""
        CASE
            WHEN avg_proteins_g > 15 THEN 'High Protein'
            WHEN avg_proteins_g > 5 THEN 'Moderate Protein'
            ELSE 'Low Protein'
        END AS protein_level""")

case_str = ",".join(case_statements) if case_statements else ""

# Build the complete SQL
if case_str:
    cat_sql = f"""
        CREATE OR REPLACE VIEW {full_schema}.vw_category_nutrition AS
        SELECT
            {cat_cols_str},{case_str}
        FROM {full_schema}.gold_category_comparison
        ORDER BY overall_health_rank
    """
else:
    cat_sql = f"""
        CREATE OR REPLACE VIEW {full_schema}.vw_category_nutrition AS
        SELECT
            {cat_cols_str}
        FROM {full_schema}.gold_category_comparison
    """

spark.sql(cat_sql)

cat_view_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_schema}.vw_category_nutrition").collect()[0]["cnt"]
print(f"vw_category_nutrition: {cat_view_count:,} categories with interpretive labels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. View: Brand Leaderboard
# MAGIC
# MAGIC Top brands ranked by average Nutri-Score, for competitive analysis dashboards.

# COMMAND ----------

# Get actual columns from gold_brand_scorecard to build dynamic view
df_brand_check = spark.table(f"{full_schema}.gold_brand_scorecard")
brand_cols = df_brand_check.columns

# Build column list based on available columns
possible_brand_columns = [
    "brands",
    "product_count",
    "avg_nutriscore_grade",
    "avg_nutriscore_numeric",
    "avg_nova_group",
    "pct_healthy",
    "pct_unhealthy",
    "pct_ultra_processed",
    "avg_energy_kcal",
    "avg_sugars_g",
    "avg_proteins_g",
]

available_brand_columns = [col for col in possible_brand_columns if col in brand_cols]
brand_cols_str = ",\n        ".join(available_brand_columns)

# Determine ORDER BY clause (use avg_nutriscore_numeric if available)
if "avg_nutriscore_numeric" in brand_cols:
    rank_clause = "DENSE_RANK() OVER (ORDER BY avg_nutriscore_numeric ASC) AS health_rank"
    order_clause = "ORDER BY health_rank"
    where_clause = "WHERE avg_nutriscore_numeric IS NOT NULL"
else:
    # Fallback if no numeric score available
    rank_clause = "ROW_NUMBER() OVER (ORDER BY brands) AS row_num"
    order_clause = "ORDER BY row_num"
    where_clause = ""

spark.sql(f"""
    CREATE OR REPLACE VIEW {full_schema}.vw_brand_leaderboard AS
    SELECT
        {brand_cols_str},
        {rank_clause}
    FROM {full_schema}.gold_brand_scorecard
    {where_clause}
    {order_clause}
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
# MAGIC > All views are registered in Unity Catalog under `workspace.food_intelligence`, giving us
# MAGIC > centralized governance — access control, lineage tracking, and discoverability in a single
# MAGIC > three-level namespace."
