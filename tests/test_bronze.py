"""
Bronze layer tests — validate raw data ingestion integrity.

These tests verify:
- Data was ingested (non-zero row count)
- Metadata columns were added correctly
- Schema contains expected key fields
- Primary key (barcode) is populated
"""

import pytest
from pyspark.sql import functions as F


class TestBronzeIngestion:
    """Tests for the Bronze layer (01_bronze_ingestion.py)."""

    def test_bronze_has_data(self, spark, bronze_path):
        """Bronze table should have a non-zero row count."""
        df = spark.read.format("delta").load(bronze_path)
        count = df.count()
        assert count > 0, f"Bronze table is empty (0 rows)"

    def test_metadata_columns_exist(self, spark, bronze_path):
        """Bronze should have the 4 pipeline metadata columns."""
        df = spark.read.format("delta").load(bronze_path)
        expected = {"_ingestion_timestamp", "_source_file", "_batch_id", "_source_format"}
        actual = set(df.columns)
        missing = expected - actual
        assert len(missing) == 0, f"Missing metadata columns: {missing}"

    def test_barcode_column_exists(self, spark, bronze_path):
        """Primary key column 'code' should exist in Bronze."""
        df = spark.read.format("delta").load(bronze_path)
        assert "code" in df.columns, "'code' column not found in Bronze schema"

    def test_barcode_mostly_populated(self, spark, bronze_path):
        """Barcode (code) should be non-null for >95% of rows."""
        df = spark.read.format("delta").load(bronze_path)
        total = df.count()
        nulls = df.filter(F.col("code").isNull()).count()
        null_pct = (nulls / total) * 100
        assert null_pct < 5, f"Barcode is {null_pct:.1f}% null (expected <5%)"

    def test_ingestion_timestamp_populated(self, spark, bronze_path):
        """All rows should have an ingestion timestamp."""
        df = spark.read.format("delta").load(bronze_path)
        nulls = df.filter(F.col("_ingestion_timestamp").isNull()).count()
        assert nulls == 0, f"{nulls:,} rows have null _ingestion_timestamp"

    def test_batch_id_consistent(self, spark, bronze_path):
        """All rows in a single ingestion should share the same batch ID."""
        df = spark.read.format("delta").load(bronze_path)
        batch_count = df.select("_batch_id").distinct().count()
        # After a full overwrite, there should be exactly 1 batch ID
        assert batch_count == 1, f"Expected 1 batch ID, found {batch_count}"

    def test_key_source_columns_present(self, spark, bronze_path):
        """Bronze should contain the key source columns we need for Silver."""
        df = spark.read.format("delta").load(bronze_path)
        # These are critical columns — without them, Silver can't function
        critical = ["code", "product_name", "brands", "countries_tags"]
        actual = set(df.columns)
        missing = [c for c in critical if c not in actual]
        assert len(missing) == 0, f"Critical columns missing from Bronze: {missing}"
