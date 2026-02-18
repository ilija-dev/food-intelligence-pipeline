# Certification Mapping: DP-600 & DP-700

How each pipeline component maps to Microsoft certification exam topics.

---

## DP-600: Implementing Analytics Solutions Using Microsoft Fabric

| Exam Domain | Pipeline Component | What to Say |
|-------------|-------------------|-------------|
| **Get data from data sources** | `00_setup.py`, `01_bronze_ingestion.py` | "I ingested external JSONL/Parquet data into a lakehouse using Spark readers with schema inference" |
| **Transform data using Spark** | `02_silver_cleaning.py` | "I used Window functions for dedup, UDFs for category parsing, and cast operations for type enforcement" |
| **Clean and transform data** | `02_silver_cleaning.py`, `03_silver_enrichment.py` | "I handled nulls, validated against allowed-value lists, and derived business columns like health tier" |
| **Create and manage lakehouses** | `00_setup.py`, all Delta writes | "I created databases, managed Delta tables in DBFS, and registered them in the metastore" |
| **Create measures and metrics** | `04_gold_nutrition.py`, `05_gold_food_quality.py` | "I built aggregated KPIs: avg nutrition per category, brand health scores, country-level distributions" |
| **Create semantic models** | `08_gold_views.py` | "I created SQL views as a semantic layer so analysts query clean abstractions, not raw tables" |
| **Validate data pipelines** | `07_data_quality.py`, `tests/` | "I built automated quality gates: row counts, zero-dupe checks, range validation, KPI sanity checks" |

## DP-700: Implementing Data Engineering Solutions Using Microsoft Fabric

| Exam Domain | Pipeline Component | What to Say |
|-------------|-------------------|-------------|
| **Ingest and transform data** | `00_setup.py`, `01_bronze_ingestion.py` | "I staged data in DBFS from external sources and loaded into Delta with metadata columns for lineage" |
| **Implement Delta Lake** | All notebooks | "I used Delta for ACID transactions, schema evolution (overwriteSchema), and time travel (DESCRIBE HISTORY)" |
| **Optimize Spark transforms** | `02_silver_cleaning.py` | "Column pruning (200+ to 40), partitioning by country, Window functions over groupBy for dedup" |
| **Design Silver layer** | `02_silver_cleaning.py`, `03_silver_enrichment.py` | "Dedup, flatten nested JSON, standardize multilingual data, validate types, derive business columns" |
| **Build consumption layer** | `04-06`, `08` Gold notebooks | "Pre-aggregated tables for specific personas, minimum sample thresholds, materialized vs view decision" |
| **Optimize query patterns** | `08_gold_views.py`, partitioning | "Materialized expensive+frequent queries, views for ad-hoc, partitioned Silver by country for pruning" |
| **Monitor data quality** | `07_data_quality.py`, `tests/` | "20+ automated checks per run, JSON reports saved to DBFS, pytest suite for CI/CD integration" |
| **Optimize Delta Lake** | Silver partitioning, config | "Partition by high-cardinality filter column, column pruning before transforms, schema evolution for source changes" |

---

## Exam-Style Questions This Project Helps You Answer

### DP-600

1. **"You need to ingest semi-structured JSON data into a lakehouse. What approach do you use?"**
   - `spark.read.json()` with auto schema inference, write as Delta with `overwriteSchema=true`

2. **"How do you remove duplicate records while preserving the most complete data?"**
   - Window function partitioned by key, ordered by completeness DESC, filter to row_number = 1

3. **"You need to create a measure that shows average nutrition per category, excluding categories with fewer than 50 products."**
   - `groupBy('category').agg(avg(...), count(...)).filter(col('count') >= 50)`

4. **"How do you create a semantic layer for business users?"**
   - SQL views with interpretive columns (CASE statements for "High Sugar"/"Low Sugar" labels)

### DP-700

1. **"What are the benefits of Delta Lake over Parquet for a data lakehouse?"**
   - ACID transactions, schema evolution, time travel, MERGE for upserts, OPTIMIZE/ZORDER for performance

2. **"How do you design a Silver layer for data with quality issues?"**
   - Validate against config-driven rules, null invalid values with correction flags, compute quality scores, partition by query pattern

3. **"When should you materialize a Gold table vs use a view?"**
   - Materialize: expensive aggregation + frequent access. View: ad-hoc, cheap joins, or composing existing Gold tables.

4. **"How do you monitor data quality in a pipeline?"**
   - Automated checks per layer (row counts, zero dupes, range validation, KPI sanity), save reports as JSON, alert on failures

5. **"How do you optimize a Delta table for query performance?"**
   - Partition by high-cardinality filter columns, ZORDER by frequently filtered non-partition columns, column pruning in Silver
