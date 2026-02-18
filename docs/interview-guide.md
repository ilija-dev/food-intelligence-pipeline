# Interview Guide: Food Intelligence Pipeline

How to present this project in data engineering interviews.
You don't memorize 20 files -- you memorize the story.

---

## The 60-Second Pitch

Use this when they say "Walk me through a project you've built."

> "I built a data pipeline processing the entire Open Food Facts database -- 3 million
> food products from 150 countries. It's a medallion architecture on Databricks with
> Delta Lake.
>
> Bronze preserves the raw data untouched -- all 200+ columns, nested JSON, duplicates.
> It's my insurance policy if anything breaks downstream.
>
> Silver is where the real engineering happens. I deduplicate by barcode using Window
> functions ranked by data completeness. I flatten nested nutriment structs, standardize
> multilingual category tags into clean English taxonomy, and validate all health scores
> against allowed-value lists. Invalid nutrition values get nulled but flagged -- I never
> silently delete data.
>
> Gold serves 8 pre-aggregated tables answering business questions like 'which brands
> are healthiest' and 'which countries have the most ultra-processed food.' All Gold
> tables enforce minimum sample sizes to prevent misleading statistics.
>
> I have 38 automated tests validating every layer -- zero duplicate barcodes, valid
> score ranges, KPI sanity checks, cross-layer consistency."

**Time**: ~45 seconds. Leaves room for follow-up.

---

## The Story Structure

Every answer follows this pattern: **Problem > Decision > Result**

Don't say: "I used a Window function."
Say: "Multiple people scan the same barcode in Open Food Facts, creating duplicates.
I used a Window function ranked by completeness and freshness to keep the best record
per product. After dedup, the dataset went from 3M to about 2.85M rows."

---

## Common Interview Questions and Answers

### Architecture & Design

**Q: Why medallion architecture?**

> "Medallion separates concerns. Bronze is insurance -- raw data I can always replay
> from. Silver is where data becomes trustworthy. Gold is where it becomes useful.
> With Open Food Facts, this matters because the raw data has 200+ columns with nested
> JSON, duplicates, and missing data in 15+ languages. You can't query that directly."

**Q: Why three layers instead of just raw and clean?**

> "Two layers collapses trust and usefulness into one table. Silver serves many
> Gold tables -- nutrition analysis, allergen reports, brand scorecards all read
> from the same Silver. If I put aggregation logic in Silver, I'd need a different
> Silver for each use case. The three-layer pattern gives me one trusted source
> feeding many business-specific outputs."

**Q: Why Delta Lake instead of just Parquet?**

> "Three reasons on this dataset specifically. First, ACID transactions -- if my
> Silver transform fails midway through 3 million records, the table isn't corrupted.
> Second, schema evolution -- Open Food Facts adds new fields without warning, and
> `mergeSchema` handles that gracefully. Third, time travel -- I can compare yesterday's
> nutrition aggregations with today's after a re-ingestion."

**Q: How is configuration managed?**

> "Everything is config-driven through a single YAML file -- source paths, quality
> thresholds, column lists, partitioning strategy. Switching from a 1GB Parquet dev
> dataset to the full 43GB JSONL production dataset is a one-line change, not a
> refactor of 9 notebooks."

---

### Data Quality & Cleaning

**Q: How do you handle duplicates?**

> "Open Food Facts is crowd-sourced, so multiple people scan the same barcode. I
> deduplicate using a Window function partitioned by barcode, ordering by completeness
> score descending, then last-modified date as a tiebreaker. Row number 1 wins.
> This keeps the entire best row intact."

**Q: Why Window function instead of groupBy?**

> "With `groupBy('code').agg(max('completeness'))`, I'd get the max completeness value
> but then need to decide what to do with product_name, brands, and 37 other columns.
> Take `first()`? That's arbitrary. `max()`? Doesn't apply to strings. Window with
> `row_number()` keeps the winning row as one coherent record."

**Q: How do you handle bad data?**

> "I never silently delete. Negative nutrition values get set to null with a
> `_nutrition_was_corrected` flag. Out-of-range scores get nulled but the row stays.
> This way, a product with bad calorie data but valid protein data still contributes
> to protein analysis. I also compute a `data_quality_score` per record so Gold
> aggregations can filter to high-quality records when needed."

**Q: What data quality issues did you find?**

> "All real, nothing fabricated. About 40% of products have incomplete nutrition
> panels. Category tags mix languages -- `en:cereals` next to `fr:Pizzas surgelees`.
> Nutrition values sometimes arrive as strings with units like '250 kcal'. Some
> products have negative calorie values or energy over 4000 kcal/100g. And the
> crowd-sourced nature means duplicate barcodes where different people scanned
> the same product with different levels of detail."

**Q: How do you test data pipelines?**

> "Two approaches for two contexts. An interactive Databricks notebook runs 20+
> checks after every pipeline execution -- row counts, zero dupes, valid ranges,
> KPI sanity. Then a separate pytest suite with 38 tests runs headless in CI.
> Same assertions, different execution contexts. The notebook is for humans during
> development, pytest is for machines in CI/CD."

---

### Technical Implementation

**Q: Experience with semi-structured data?**

> "The Open Food Facts JSONL has a `nutriments` object with 100+ nested fields,
> many legacy or empty. In Bronze I preserve the full nested structure. In Silver
> I flatten the 10 fields I need into typed Double columns. Spark's `spark.read.json()`
> auto-infers the nested schema, and I use backtick notation for column names with
> hyphens, like `energy-kcal_100g`."

**Q: How do you handle internationalized data?**

> "Category tags come with language prefixes: `en:breakfast-cereals`, `fr:pizzas`,
> `de:getranke`. I wrote a UDF that extracts English tags first, falls back to any
> language if no English tag exists, and cleans the result into a readable format.
> The most specific (last) tag in the taxonomy becomes `primary_category`. Same
> pattern for countries: `en:france` becomes `France`."

**Q: How do you optimize query performance?**

> "Two main strategies. First, Silver is partitioned by `primary_country` because
> most Gold queries filter by region -- this gives partition pruning, so a query
> about France only scans France's files, not all 150 countries. Second, I prune
> columns aggressively -- selecting 40 from 200+ before any transformation, so
> Spark never shuffles data it doesn't need. In production I'd add ZORDER on
> `primary_category` for point lookups."

**Q: When do you materialize a Gold table vs use a SQL view?**

> "Materialize when the query is expensive AND runs frequently. Brand scorecard
> scans 3 million Silver rows -- if dashboards hit that hourly, materialize it.
> Country health overview joins three small Gold tables -- that's cheap, so it's
> a view with zero storage cost. The decision matrix is cost times frequency."

---

### Scale & Production

**Q: Have you worked with data at scale?**

> "The full Open Food Facts dataset is 43GB decompressed with 3 million products.
> I had to think about partitioning, column pruning, and write strategies. On
> Databricks Community Edition I used the 1.2GB Parquet for development and the
> full JSONL for the production run. The pipeline is designed so switching between
> them is a one-line config change."

**Q: What would you change for production?**

> "Three things. First, switch Bronze from full overwrite to Delta merge (upsert)
> so I'm only processing new and updated products daily instead of re-ingesting
> everything. Second, add Databricks Workflows or Azure Data Factory for
> orchestration with retry logic and alerting on failures. Third, add ZORDER
> on frequently filtered columns and tune partition sizes based on actual query
> patterns."

**Q: How would you monitor this in production?**

> "The quality notebook already saves a JSON report to DBFS after every run --
> total checks, pass count, fail count, individual results. In production I'd
> wire that to Databricks Workflows alerting: if pass rate drops below 100%,
> send a Slack notification. The pytest suite would run in a CI pipeline on every
> merge to main. Delta Lake's `DESCRIBE HISTORY` gives me a full audit trail
> of every write."

---

### Business & Domain

**Q: What business problems does this solve?**

> "Three personas. Health regulators can track ultra-processed food trends by
> country and see which nations have the worst NOVA 4 percentages. Food companies
> get competitive analysis -- how does their brand's nutrition profile compare to
> competitors. Consumers with dietary restrictions can find allergen-free options
> by category. All three use cases query the same Gold layer through different
> views and tables."

**Q: What's the most interesting finding in the data?**

> "The Nutri-Score vs NOVA cross-tabulation. You'd expect ultra-processed food
> (NOVA 4) to always score poorly on nutrition (Nutri-Score D or E). But diet
> sodas are NOVA 4 -- full of additives and artificial sweeteners -- yet score
> Nutri-Score A because they have zero calories and no sugar. This isn't a data
> bug, it's a real tension in food classification systems. Building that Gold
> table lets regulators quantify exactly how many products fall into each
> quadrant of that debate."

---

## Key Numbers to Remember

| Metric | Value |
|--------|-------|
| Total products | 3M+ |
| Countries | 150+ |
| Raw columns | 200+ |
| Silver columns | ~40 |
| Gold tables | 8 |
| SQL views | 5 |
| Automated tests | 38 |
| Quality checks | 20+ |
| Dedup method | Window function (completeness + freshness) |
| Partition column | `primary_country` |
| Config approach | Single YAML file |

---

## What to Emphasize vs Skip

### Spend 80% of your time on:
- **Silver layer** -- this is where the engineering is
- **Real data quality issues** -- interviewers love hearing about actual problems, not textbook ones
- **Design decisions** -- why Window over groupBy, why partition by country, why config-driven
- **Testing approach** -- most candidates skip this entirely

### Mention briefly:
- Bronze (one sentence: "raw data, zero transforms, metadata for lineage")
- Gold tables (one sentence: "pre-aggregated for business questions with min sample thresholds")
- Tech stack (PySpark, Delta Lake, Databricks -- they can see this on the resume)

### Don't bring up unless asked:
- Specific column names
- YAML config structure details
- Individual SQL queries
- Dashboard specifications

---

## Mapping to Resume Bullets

Use these as resume bullet points:

- "Designed and built a medallion architecture (Bronze/Silver/Gold) processing 3M+ food products from 150+ countries using PySpark and Delta Lake on Databricks"
- "Implemented data quality pipeline with 38 automated tests and 20+ validation checks across all layers, achieving zero-duplicate guarantee on Silver output"
- "Resolved real-world data quality challenges including crowd-sourced duplicates, nested JSON flattening, multilingual category standardization, and invalid value handling"
- "Created config-driven pipeline where switching between 1GB dev and 43GB production datasets requires a single YAML change"
- "Built 8 Gold analytics tables with minimum sample thresholds and 5 SQL views serving health regulators, competitive analysts, and consumers"
