-- =============================================================================
-- Gold Layer Analytical Queries
-- =============================================================================
-- Ready-to-run SQL queries for Databricks SQL Editor or dashboards.
-- These query the Gold tables and views built by the pipeline.
-- Copy-paste into Databricks SQL to demo the pipeline output.
--
-- Prerequisites: Run notebooks 00 through 08 first.
-- =============================================================================

USE food_intelligence;

-- ---------------------------------------------------------------------------
-- 1. TOP 20 HEALTHIEST FOOD CATEGORIES
-- Business question: Which food categories have the best nutrition profile?
-- ---------------------------------------------------------------------------

SELECT
    primary_category,
    product_count,
    avg_energy_kcal,
    avg_sugars_g,
    avg_proteins_g,
    avg_fiber_g,
    overall_health_rank
FROM gold_category_comparison
ORDER BY overall_health_rank ASC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 2. TOP 20 UNHEALTHIEST FOOD CATEGORIES
-- Business question: Which categories should regulators watch?
-- ---------------------------------------------------------------------------

SELECT
    primary_category,
    product_count,
    avg_energy_kcal,
    avg_sugars_g,
    avg_fat_g,
    avg_salt_g,
    overall_health_rank
FROM gold_category_comparison
ORDER BY overall_health_rank DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 3. BRAND HEALTH LEADERBOARD — TOP 25 HEALTHIEST BRANDS
-- Business question: Which brands consistently produce healthy products?
-- ---------------------------------------------------------------------------

SELECT
    brands,
    product_count,
    avg_nutriscore_grade,
    avg_nova_group,
    ROUND(pct_healthy * 100, 1) AS pct_healthy,
    ROUND(pct_ultra_processed * 100, 1) AS pct_ultra_processed,
    avg_energy_kcal,
    avg_sugars_g
FROM gold_brand_scorecard
WHERE avg_nutriscore_numeric IS NOT NULL
ORDER BY avg_nutriscore_numeric ASC
LIMIT 25;


-- ---------------------------------------------------------------------------
-- 4. BRAND HEALTH LEADERBOARD — BOTTOM 25 (WORST SCORING)
-- Business question: Which brands have the worst nutrition profiles?
-- ---------------------------------------------------------------------------

SELECT
    brands,
    product_count,
    avg_nutriscore_grade,
    avg_nova_group,
    ROUND(pct_unhealthy * 100, 1) AS pct_unhealthy,
    ROUND(pct_ultra_processed * 100, 1) AS pct_ultra_processed,
    avg_energy_kcal,
    avg_sugars_g
FROM gold_brand_scorecard
WHERE avg_nutriscore_numeric IS NOT NULL
ORDER BY avg_nutriscore_numeric DESC
LIMIT 25;


-- ---------------------------------------------------------------------------
-- 5. COUNTRY NUTRI-SCORE COMPARISON: FRANCE vs USA vs GERMANY vs UK
-- Business question: Which country has the healthiest food supply?
-- ---------------------------------------------------------------------------

SELECT
    primary_country,
    nutriscore_grade,
    product_count,
    pct_of_country
FROM gold_country_nutriscore
WHERE primary_country IN ('France', 'United States', 'Germany', 'United Kingdom')
ORDER BY primary_country, nutriscore_grade;


-- ---------------------------------------------------------------------------
-- 6. ULTRA-PROCESSED FOOD BY COUNTRY — TOP 15
-- Business question: Which countries have the most ultra-processed food?
-- ---------------------------------------------------------------------------

SELECT
    primary_country,
    total_products,
    pct_nova_4_ultra_processed,
    pct_nova_1_unprocessed,
    avg_nova_group
FROM gold_ultra_processing_by_country
ORDER BY pct_nova_4_ultra_processed DESC
LIMIT 15;


-- ---------------------------------------------------------------------------
-- 7. THE NUTRI-SCORE vs NOVA PARADOX
-- Business question: Can ultra-processed food score well on nutrition?
-- ---------------------------------------------------------------------------

SELECT
    nova_group,
    nutriscore_grade,
    product_count,
    pct_within_nova
FROM gold_nutriscore_vs_nova
ORDER BY nova_group, nutriscore_grade;


-- ---------------------------------------------------------------------------
-- 8. GLUTEN PREVALENCE ACROSS CATEGORIES
-- Business question: Which food categories contain the most gluten?
-- ---------------------------------------------------------------------------

SELECT
    primary_category,
    products_with_allergen,
    total_products_in_category,
    prevalence_pct
FROM gold_allergen_prevalence
WHERE allergen = 'Gluten'
ORDER BY prevalence_pct DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 9. DAIRY-FREE OPTIONS BY CATEGORY
-- Business question: Where can lactose-intolerant consumers find options?
-- ---------------------------------------------------------------------------

SELECT
    primary_category,
    total_with_allergen_data,
    free_from_count,
    pct_free_from
FROM gold_allergen_free_options
WHERE allergen = 'Milk'
ORDER BY pct_free_from DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 10. FULL COUNTRY HEALTH OVERVIEW
-- Business question: One-page summary of food health by country
-- ---------------------------------------------------------------------------

SELECT
    primary_country,
    total_products,
    avg_nova_group,
    pct_nova_4_ultra_processed,
    pct_a AS pct_nutriscore_a,
    pct_e AS pct_nutriscore_e
FROM vw_country_health_overview
ORDER BY total_products DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 11. HIGH PROTEIN, LOW SUGAR PRODUCTS (Product Discovery)
-- Business question: What are the healthiest products in the database?
-- ---------------------------------------------------------------------------

SELECT
    code,
    product_name,
    brands,
    primary_category,
    proteins_100g,
    sugars_100g,
    energy_kcal_100g,
    nutriscore_grade,
    nova_group,
    health_tier
FROM vw_product_search
WHERE proteins_100g > 20
  AND sugars_100g < 5
  AND nutriscore_grade IN ('a', 'b')
  AND data_quality_score >= 0.7
ORDER BY proteins_100g DESC
LIMIT 25;


-- ---------------------------------------------------------------------------
-- 12. SUGAR CONTENT BY CATEGORY — COMPARISON
-- Business question: How do "healthy" categories compare on sugar?
-- ---------------------------------------------------------------------------

SELECT
    primary_category,
    product_count,
    avg_sugars_g,
    stddev_sugars_g,
    sugar_rank
FROM gold_category_comparison
WHERE primary_category IN (
    'breakfast-cereals', 'granolas', 'mueslis',
    'fruit-juices', 'sodas', 'yogurts',
    'chocolate-bars', 'biscuits', 'ice-creams'
)
ORDER BY avg_sugars_g ASC;
