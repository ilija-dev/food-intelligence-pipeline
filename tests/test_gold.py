"""
Gold layer tests — validate aggregation correctness and business rules.

These tests verify:
- Gold tables exist and have data
- Minimum product thresholds are enforced
- Percentages sum correctly
- KPI values are within reasonable domain bounds
- Cross-layer consistency (Gold categories trace back to Silver)
"""

import pytest
from pyspark.sql import functions as F


class TestGoldNutrition:
    """Tests for Gold nutrition tables (04_gold_nutrition.py)."""

    def test_nutrition_by_category_has_data(self, spark, gold_paths):
        """gold_nutrition_by_category should have data."""
        df = spark.read.format("delta").load(gold_paths["nutrition_by_category"])
        assert df.count() > 0, "nutrition_by_category is empty"

    def test_nutrition_min_product_threshold(self, spark, gold_paths, quality_config):
        """All categories should meet the minimum product count threshold."""
        df = spark.read.format("delta").load(gold_paths["nutrition_by_category"])
        min_count = quality_config["min_products_per_category"]
        below = df.filter(F.col("product_count") < min_count).count()
        assert below == 0, f"{below} categories below threshold of {min_count}"

    def test_brand_scorecard_has_data(self, spark, gold_paths):
        """gold_brand_scorecard should have data."""
        df = spark.read.format("delta").load(gold_paths["brand_scorecard"])
        assert df.count() > 0, "brand_scorecard is empty"

    def test_brand_min_product_threshold(self, spark, gold_paths, quality_config):
        """All brands should meet the minimum product count threshold."""
        df = spark.read.format("delta").load(gold_paths["brand_scorecard"])
        min_count = quality_config["min_products_per_brand"]
        below = df.filter(F.col("product_count") < min_count).count()
        assert below == 0, f"{below} brands below threshold of {min_count}"

    def test_brand_nutriscore_grade_valid(self, spark, gold_paths):
        """Average Nutri-Score grade should be a valid letter."""
        df = spark.read.format("delta").load(gold_paths["brand_scorecard"])
        valid = {"a", "b", "c", "d", "e"}
        invalid = (
            df.filter(F.col("avg_nutriscore_grade").isNotNull())
            .filter(~F.col("avg_nutriscore_grade").isin(list(valid)))
            .count()
        )
        assert invalid == 0, f"{invalid} brands have invalid avg_nutriscore_grade"

    def test_category_comparison_has_ranks(self, spark, gold_paths):
        """gold_category_comparison should have health ranking columns."""
        df = spark.read.format("delta").load(gold_paths["category_comparison"])
        assert df.count() > 0, "category_comparison is empty"
        rank_cols = {"sugar_rank", "calorie_rank", "protein_rank", "fiber_rank", "overall_health_rank"}
        actual = set(df.columns)
        missing = rank_cols - actual
        assert len(missing) == 0, f"Missing rank columns: {missing}"

    def test_avg_calories_reasonable(self, spark, gold_paths):
        """Overall average calories across categories should be 50-500 kcal."""
        df = spark.read.format("delta").load(gold_paths["nutrition_by_category"])
        avg = df.agg(F.avg("avg_energy_kcal")).collect()[0][0]
        assert avg is not None, "avg_energy_kcal is entirely null"
        assert 50 <= avg <= 500, f"Overall avg calories is {avg:.1f} (expected 50-500)"


class TestGoldFoodQuality:
    """Tests for Gold food quality tables (05_gold_food_quality.py)."""

    def test_country_nutriscore_has_data(self, spark, gold_paths):
        """gold_country_nutriscore should have data."""
        df = spark.read.format("delta").load(gold_paths["country_nutriscore"])
        assert df.count() > 0, "country_nutriscore is empty"

    def test_country_nutriscore_percentages_sum(self, spark, gold_paths):
        """Per-country Nutri-Score percentages should sum to ~100%."""
        df = spark.read.format("delta").load(gold_paths["country_nutriscore"])
        sums = (
            df.groupBy("primary_country")
            .agg(F.round(F.sum("pct_of_country"), 1).alias("total"))
        )
        bad = sums.filter((F.col("total") < 99.0) | (F.col("total") > 101.0)).count()
        assert bad == 0, f"{bad} countries have Nutri-Score percentages not summing to ~100%"

    def test_country_nutriscore_valid_grades(self, spark, gold_paths):
        """All grades in country_nutriscore should be a-e."""
        df = spark.read.format("delta").load(gold_paths["country_nutriscore"])
        valid = {"a", "b", "c", "d", "e"}
        invalid = df.filter(~F.col("nutriscore_grade").isin(list(valid))).count()
        assert invalid == 0, f"{invalid} rows have invalid nutriscore_grade"

    def test_ultra_processing_has_data(self, spark, gold_paths):
        """gold_ultra_processing_by_country should have data."""
        df = spark.read.format("delta").load(gold_paths["ultra_processing_by_country"])
        assert df.count() > 0, "ultra_processing_by_country is empty"

    def test_ultra_processing_percentages_bounded(self, spark, gold_paths):
        """NOVA percentages should be between 0 and 100."""
        df = spark.read.format("delta").load(gold_paths["ultra_processing_by_country"])
        pct_cols = [
            "pct_nova_1_unprocessed",
            "pct_nova_2_processed_ingredients",
            "pct_nova_3_processed",
            "pct_nova_4_ultra_processed",
        ]
        for col in pct_cols:
            if col in df.columns:
                out = df.filter((F.col(col) < 0) | (F.col(col) > 100)).count()
                assert out == 0, f"{col} has {out} values outside [0, 100]"

    def test_nutriscore_vs_nova_has_data(self, spark, gold_paths):
        """gold_nutriscore_vs_nova should have cross-tab data."""
        df = spark.read.format("delta").load(gold_paths["nutriscore_vs_nova"])
        count = df.count()
        # Should have at least 10 of the possible 20 cells (5 grades x 4 NOVA)
        assert count >= 10, f"Only {count} cells in cross-tab (expected >= 10)"


class TestGoldAllergens:
    """Tests for Gold allergen tables (06_gold_allergens.py)."""

    def test_allergen_prevalence_has_data(self, spark, gold_paths):
        """gold_allergen_prevalence should have data."""
        df = spark.read.format("delta").load(gold_paths["allergen_prevalence"])
        assert df.count() > 0, "allergen_prevalence is empty"

    def test_allergen_free_has_data(self, spark, gold_paths):
        """gold_allergen_free_options should have data."""
        df = spark.read.format("delta").load(gold_paths["allergen_free_options"])
        assert df.count() > 0, "allergen_free_options is empty"

    def test_prevalence_pct_bounded(self, spark, gold_paths):
        """Allergen prevalence percentages should be between 0 and 100."""
        df = spark.read.format("delta").load(gold_paths["allergen_prevalence"])
        out = df.filter(
            (F.col("prevalence_pct") < 0) | (F.col("prevalence_pct") > 100)
        ).count()
        assert out == 0, f"{out} rows have prevalence_pct outside [0, 100]"

    def test_free_from_pct_bounded(self, spark, gold_paths):
        """Free-from percentages should be between 0 and 100."""
        df = spark.read.format("delta").load(gold_paths["allergen_free_options"])
        out = df.filter(
            (F.col("pct_free_from") < 0) | (F.col("pct_free_from") > 100)
        ).count()
        assert out == 0, f"{out} rows have pct_free_from outside [0, 100]"


class TestCrossLayerConsistency:
    """Tests that verify consistency between layers."""

    def test_gold_categories_in_silver(self, spark, silver_path, gold_paths):
        """Every category in Gold should exist in Silver."""
        gold_cats = set(
            spark.read.format("delta")
            .load(gold_paths["nutrition_by_category"])
            .select("primary_category")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        silver_cats = set(
            spark.read.format("delta")
            .load(silver_path)
            .select("primary_category")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        orphans = gold_cats - silver_cats
        assert len(orphans) == 0, f"Gold has {len(orphans)} categories not in Silver: {orphans}"

    def test_silver_leq_bronze(self, spark, bronze_path, silver_path):
        """Silver row count should not exceed Bronze."""
        bronze = spark.read.format("delta").load(bronze_path).count()
        silver = spark.read.format("delta").load(silver_path).count()
        assert silver <= bronze, f"Silver ({silver:,}) > Bronze ({bronze:,})"
