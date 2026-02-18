"""
Silver layer tests — validate cleaning and enrichment correctness.

These tests verify:
- Zero duplicate barcodes (dedup worked)
- All categorical scores in valid ranges
- No negative or out-of-range nutrition values
- Enrichment columns exist and are bounded correctly
- Row count is <= Bronze (we remove, never add)
"""

import pytest
from pyspark.sql import functions as F


class TestSilverCleaning:
    """Tests for Silver cleaning (02_silver_cleaning.py)."""

    def test_silver_has_data(self, spark, silver_path):
        """Silver table should have a non-zero row count."""
        df = spark.read.format("delta").load(silver_path)
        assert df.count() > 0, "Silver table is empty"

    def test_silver_leq_bronze(self, spark, bronze_path, silver_path):
        """Silver should have fewer or equal rows than Bronze (dedup removes rows)."""
        bronze_count = spark.read.format("delta").load(bronze_path).count()
        silver_count = spark.read.format("delta").load(silver_path).count()
        assert silver_count <= bronze_count, (
            f"Silver ({silver_count:,}) has MORE rows than Bronze ({bronze_count:,})"
        )

    def test_zero_duplicate_barcodes(self, spark, silver_path):
        """No barcode should appear more than once in Silver."""
        df = spark.read.format("delta").load(silver_path)
        dupes = df.groupBy("code").count().filter(F.col("count") > 1).count()
        assert dupes == 0, f"Found {dupes:,} duplicate barcodes in Silver"

    def test_nutriscore_valid_range(self, spark, silver_path, quality_config):
        """All non-null Nutri-Score grades should be a, b, c, d, or e."""
        df = spark.read.format("delta").load(silver_path)
        valid = quality_config["valid_nutriscore_grades"]
        invalid = (
            df.filter(F.col("nutriscore_grade").isNotNull())
            .filter(~F.col("nutriscore_grade").isin(valid))
            .count()
        )
        assert invalid == 0, f"Found {invalid:,} invalid Nutri-Score values"

    def test_nova_group_valid_range(self, spark, silver_path, quality_config):
        """All non-null NOVA groups should be 1, 2, 3, or 4."""
        df = spark.read.format("delta").load(silver_path)
        valid = quality_config["valid_nova_groups"]
        invalid = (
            df.filter(F.col("nova_group").isNotNull())
            .filter(~F.col("nova_group").isin(valid))
            .count()
        )
        assert invalid == 0, f"Found {invalid:,} invalid NOVA group values"

    def test_ecoscore_valid_range(self, spark, silver_path, quality_config):
        """All non-null Eco-Score grades should be a, b, c, d, or e."""
        df = spark.read.format("delta").load(silver_path)
        if "ecoscore_grade" not in df.columns:
            pytest.skip("ecoscore_grade not in Silver schema")
        valid = quality_config["valid_ecoscore_grades"]
        invalid = (
            df.filter(F.col("ecoscore_grade").isNotNull())
            .filter(~F.col("ecoscore_grade").isin(valid))
            .count()
        )
        assert invalid == 0, f"Found {invalid:,} invalid Eco-Score values"

    @pytest.mark.parametrize(
        "column",
        [
            "energy_kcal_100g",
            "fat_100g",
            "saturated_fat_100g",
            "carbohydrates_100g",
            "sugars_100g",
            "fiber_100g",
            "proteins_100g",
            "salt_100g",
        ],
    )
    def test_no_negative_nutrition(self, spark, silver_path, column):
        """No nutrition column should have negative values."""
        df = spark.read.format("delta").load(silver_path)
        if column not in df.columns:
            pytest.skip(f"{column} not in Silver schema")
        neg_count = df.filter(F.col(column) < 0).count()
        assert neg_count == 0, f"{column} has {neg_count:,} negative values"

    def test_energy_within_max(self, spark, silver_path, quality_config):
        """Energy kcal should not exceed configured maximum."""
        df = spark.read.format("delta").load(silver_path)
        max_energy = quality_config["nutrition_ranges"]["energy_kcal"]["max"]
        over = df.filter(F.col("energy_kcal_100g") > max_energy).count()
        assert over == 0, f"{over:,} rows exceed max energy ({max_energy} kcal)"

    def test_no_null_barcodes(self, spark, silver_path):
        """Silver should have no null barcodes (they were filtered out)."""
        df = spark.read.format("delta").load(silver_path)
        nulls = df.filter(F.col("code").isNull() | (F.trim(F.col("code")) == "")).count()
        assert nulls == 0, f"{nulls:,} null/empty barcodes in Silver"


class TestSilverEnrichment:
    """Tests for Silver enrichment (03_silver_enrichment.py)."""

    def test_enrichment_columns_exist(self, spark, silver_path):
        """All 5 enrichment columns should be present."""
        df = spark.read.format("delta").load(silver_path)
        expected = {
            "nutrition_completeness",
            "is_ultra_processed",
            "health_tier",
            "data_quality_score",
            "product_age_days",
        }
        actual = set(df.columns)
        missing = expected - actual
        assert len(missing) == 0, f"Missing enrichment columns: {missing}"

    def test_nutrition_completeness_bounded(self, spark, silver_path):
        """nutrition_completeness should be between 0.0 and 1.0."""
        df = spark.read.format("delta").load(silver_path)
        out = df.filter(
            (F.col("nutrition_completeness") < 0)
            | (F.col("nutrition_completeness") > 1)
        ).count()
        assert out == 0, f"{out:,} rows have nutrition_completeness outside [0, 1]"

    def test_data_quality_score_bounded(self, spark, silver_path):
        """data_quality_score should be between 0.0 and 1.0."""
        df = spark.read.format("delta").load(silver_path)
        out = df.filter(
            (F.col("data_quality_score") < 0) | (F.col("data_quality_score") > 1)
        ).count()
        assert out == 0, f"{out:,} rows have data_quality_score outside [0, 1]"

    def test_health_tier_valid_values(self, spark, silver_path):
        """health_tier should be Healthy, Moderate, Unhealthy, or null."""
        df = spark.read.format("delta").load(silver_path)
        valid = {"Healthy", "Moderate", "Unhealthy"}
        invalid = (
            df.filter(F.col("health_tier").isNotNull())
            .filter(~F.col("health_tier").isin(list(valid)))
            .count()
        )
        assert invalid == 0, f"{invalid:,} rows have invalid health_tier values"

    def test_ultra_processed_matches_nova(self, spark, silver_path):
        """is_ultra_processed should be True iff nova_group == 4."""
        df = spark.read.format("delta").load(silver_path)
        # Check: nova_group=4 but is_ultra_processed != True
        mismatch = df.filter(
            (F.col("nova_group") == 4) & (F.col("is_ultra_processed") != True)  # noqa: E712
        ).count()
        assert mismatch == 0, f"{mismatch:,} rows where nova=4 but is_ultra_processed is not True"

        # Check: is_ultra_processed=True but nova_group != 4
        mismatch2 = df.filter(
            (F.col("is_ultra_processed") == True) & (F.col("nova_group") != 4)  # noqa: E712
        ).count()
        assert mismatch2 == 0, f"{mismatch2:,} rows where is_ultra_processed but nova != 4"

    def test_product_age_non_negative(self, spark, silver_path):
        """product_age_days should not be negative."""
        df = spark.read.format("delta").load(silver_path)
        neg = df.filter(F.col("product_age_days") < 0).count()
        assert neg == 0, f"{neg:,} rows have negative product_age_days"
