-- =============================================================================
-- Ad-Hoc Exploration Queries
-- =============================================================================
-- Use these during development to understand the data at each layer.
-- Run in Databricks SQL Editor.
-- =============================================================================

USE food_intelligence;

-- ---------------------------------------------------------------------------
-- BRONZE EXPLORATION
-- ---------------------------------------------------------------------------

-- How many raw rows do we have?
SELECT COUNT(*) AS bronze_row_count FROM bronze_products;

-- What does the schema look like? (column names and types)
DESCRIBE TABLE bronze_products;

-- Preview raw data
SELECT code, product_name, brands, categories_tags, countries_tags
FROM bronze_products
LIMIT 10;

-- Check metadata columns
SELECT DISTINCT _batch_id, _source_format, MIN(_ingestion_timestamp), MAX(_ingestion_timestamp)
FROM bronze_products
GROUP BY _batch_id, _source_format;

-- How many null barcodes in raw data?
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN code IS NULL THEN 1 ELSE 0 END) AS null_codes,
    ROUND(SUM(CASE WHEN code IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS null_pct
FROM bronze_products;


-- ---------------------------------------------------------------------------
-- SILVER EXPLORATION
-- ---------------------------------------------------------------------------

-- Row count and column count
SELECT COUNT(*) AS silver_row_count FROM silver_products;

-- Dedup verification: should return 0
SELECT COUNT(*) AS duplicate_barcodes
FROM (
    SELECT code, COUNT(*) AS cnt
    FROM silver_products
    GROUP BY code
    HAVING cnt > 1
);

-- Null rate report for key fields
SELECT
    COUNT(*) AS total,
    ROUND(SUM(CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_has_name,
    ROUND(SUM(CASE WHEN brands IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_has_brand,
    ROUND(SUM(CASE WHEN primary_category IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_has_category,
    ROUND(SUM(CASE WHEN nutriscore_grade IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_has_nutriscore,
    ROUND(SUM(CASE WHEN nova_group IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_has_nova,
    ROUND(SUM(CASE WHEN energy_kcal_100g IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_has_energy
FROM silver_products;

-- Top countries by product count
SELECT primary_country, COUNT(*) AS product_count
FROM silver_products
WHERE primary_country IS NOT NULL
GROUP BY primary_country
ORDER BY product_count DESC
LIMIT 20;

-- Top categories by product count
SELECT primary_category, COUNT(*) AS product_count
FROM silver_products
WHERE primary_category IS NOT NULL
GROUP BY primary_category
ORDER BY product_count DESC
LIMIT 20;

-- Nutrition completeness distribution
SELECT
    ROUND(nutrition_completeness, 1) AS completeness_bucket,
    COUNT(*) AS product_count
FROM silver_products
GROUP BY ROUND(nutrition_completeness, 1)
ORDER BY completeness_bucket;

-- Health tier distribution
SELECT health_tier, COUNT(*) AS product_count
FROM silver_products
GROUP BY health_tier
ORDER BY product_count DESC;

-- Data quality score distribution
SELECT
    ROUND(data_quality_score, 1) AS quality_bucket,
    COUNT(*) AS product_count
FROM silver_products
GROUP BY ROUND(data_quality_score, 1)
ORDER BY quality_bucket;

-- How many nutrition corrections were made?
SELECT
    _nutrition_was_corrected,
    COUNT(*) AS product_count
FROM silver_products
GROUP BY _nutrition_was_corrected;

-- Delta Lake history — see all operations
DESCRIBE HISTORY silver_products;


-- ---------------------------------------------------------------------------
-- GOLD EXPLORATION
-- ---------------------------------------------------------------------------

-- Quick row counts for all Gold tables
SELECT 'gold_nutrition_by_category' AS table_name, COUNT(*) AS rows FROM gold_nutrition_by_category
UNION ALL
SELECT 'gold_brand_scorecard', COUNT(*) FROM gold_brand_scorecard
UNION ALL
SELECT 'gold_category_comparison', COUNT(*) FROM gold_category_comparison
UNION ALL
SELECT 'gold_country_nutriscore', COUNT(*) FROM gold_country_nutriscore
UNION ALL
SELECT 'gold_ultra_processing_by_country', COUNT(*) FROM gold_ultra_processing_by_country
UNION ALL
SELECT 'gold_nutriscore_vs_nova', COUNT(*) FROM gold_nutriscore_vs_nova
UNION ALL
SELECT 'gold_allergen_prevalence', COUNT(*) FROM gold_allergen_prevalence
UNION ALL
SELECT 'gold_allergen_free_options', COUNT(*) FROM gold_allergen_free_options;

-- Spot check: categories with highest sugar (should see sodas, candy, etc.)
SELECT primary_category, avg_sugars_g, product_count
FROM gold_nutrition_by_category
ORDER BY avg_sugars_g DESC
LIMIT 10;

-- Spot check: categories with highest protein (should see meats, fish, etc.)
SELECT primary_category, avg_proteins_g, product_count
FROM gold_nutrition_by_category
ORDER BY avg_proteins_g DESC
LIMIT 10;
