# Dashboard Specification

## Overview

Three dashboards built in Databricks SQL Dashboards (or Power BI on Fabric).
Each targets a different persona.

---

## Dashboard 1: Food Quality by Country

**Persona**: Health regulators, public health researchers

**Data sources**: `vw_country_health_overview`, `gold_country_nutriscore`, `gold_ultra_processing_by_country`

### Visualizations

| # | Chart Type | Title | Source Query | Notes |
|---|-----------|-------|-------------|-------|
| 1 | Choropleth map | Ultra-Processed Food by Country | `gold_ultra_processing_by_country` | Color by `pct_nova_4_ultra_processed` |
| 2 | Stacked bar | Nutri-Score Distribution by Country | `gold_country_nutriscore` | Filter to top 15 countries by product count. Stack A-E grades. |
| 3 | Table | Country Health Scorecard | `vw_country_health_overview` | Sortable by any column |
| 4 | Bar chart | Average NOVA Group by Country | `gold_ultra_processing_by_country` | Horizontal bars, sorted desc |

### Filters
- Country multi-select
- Minimum product count slider (default: 100)

---

## Dashboard 2: Brand & Category Analytics

**Persona**: Food companies, competitive analysts, consumers

**Data sources**: `gold_brand_scorecard`, `gold_category_comparison`, `vw_brand_leaderboard`

### Visualizations

| # | Chart Type | Title | Source Query | Notes |
|---|-----------|-------|-------------|-------|
| 1 | Scatter plot | Brand Health vs Product Count | `gold_brand_scorecard` | X: product_count, Y: avg_nutriscore_numeric, size: pct_ultra_processed |
| 2 | Bar chart | Top 20 Healthiest Brands | `vw_brand_leaderboard` | Horizontal bars by avg_nutriscore_numeric ASC |
| 3 | Heatmap | Category Nutrition Comparison | `gold_category_comparison` | Rows: categories, Cols: nutrients, Color: relative value |
| 4 | Table | Category Nutrition with Ranks | `vw_category_nutrition` | Include interpretive labels (High Sugar, Low Calorie) |
| 5 | Bar chart | Sugar Content: "Healthy" vs "Unhealthy" Categories | `gold_category_comparison` | Compare cereals, granola, yogurt, sodas, etc. |

### Filters
- Category search/multi-select
- Brand search
- Minimum product count slider

---

## Dashboard 3: Allergen Intelligence

**Persona**: Consumers with dietary restrictions, food safety teams

**Data sources**: `gold_allergen_prevalence`, `gold_allergen_free_options`

### Visualizations

| # | Chart Type | Title | Source Query | Notes |
|---|-----------|-------|-------------|-------|
| 1 | Horizontal bar | Top 10 Allergens by Prevalence | `gold_allergen_prevalence` aggregated | Overall, across all categories |
| 2 | Heatmap | Allergen Prevalence by Category | `gold_allergen_prevalence` | Rows: categories, Cols: allergens, Color: prevalence_pct |
| 3 | Bar chart | Free-From Options by Category | `gold_allergen_free_options` | Filtered to selected allergen |
| 4 | Table | Detailed Allergen Report | `gold_allergen_prevalence` | Full drill-down table |

### Filters
- Allergen selector (Gluten, Milk, Eggs, Nuts, etc.)
- Category multi-select
- Minimum category size slider

---

## Implementation Notes

### Databricks SQL Dashboard Setup
1. Create a new SQL Dashboard in Databricks workspace
2. Add each visualization as a widget
3. Connect filters across widgets using dashboard filter parameters
4. Schedule auto-refresh (e.g., daily at 6am after pipeline runs)

### Power BI Setup (Fabric)
1. Connect to the lakehouse SQL endpoint
2. Import Gold tables as DirectQuery sources
3. Build report pages matching the dashboard specs above
4. Use Power BI slicers for the filter controls
