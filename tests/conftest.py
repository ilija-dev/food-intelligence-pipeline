"""
Shared pytest fixtures for food intelligence pipeline tests.

These tests are designed to run against live Delta tables on Databricks.
They can also run locally with a PySpark session if Delta Lake is installed.

Usage:
    # On Databricks (via dbx or databricks-connect):
    pytest tests/ -v

    # Locally (requires pyspark + delta-spark):
    pytest tests/ -v
"""

import os
import pytest
import yaml


@pytest.fixture(scope="session")
def spark():
    """Create or get a Spark session for testing."""
    try:
        # Try to get existing Databricks spark session
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        pass

    # Fall back to local PySpark
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("food-intelligence-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def config():
    """Load pipeline configuration."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "pipeline_config.yaml",
    )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def db_name(config):
    """Database name from config."""
    return config["database"]["name"]


@pytest.fixture(scope="session")
def bronze_path(config):
    """Bronze table path."""
    return config["delta_tables"]["bronze"]["products"]


@pytest.fixture(scope="session")
def silver_path(config):
    """Silver table path."""
    return config["delta_tables"]["silver"]["products"]


@pytest.fixture(scope="session")
def gold_paths(config):
    """Gold table paths."""
    return config["delta_tables"]["gold"]


@pytest.fixture(scope="session")
def quality_config(config):
    """Quality thresholds from config."""
    return config["quality"]
