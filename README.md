# Sysco Foodservice Sales Analytics
### BigQuery Star Schema + Tableau

End-to-end sales analytics pipeline built to simulate a Sysco-style foodservice 
distribution environment. Raw CSVs → BigQuery star schema → analytics views → 
executive Tableau dashboard.

---

## Dashboard

| Page | Link |
| Executive Overview | [View →](https://public.tableau.com/app/profile/hussain.zakiuddin/viz/SyscoFoodserviceSalesAnalyticsBigQueryStarSchemaTableau/ExecutiveOverview) |
| Sales Performance Deep Dive | [View →](https://public.tableau.com/app/profile/hussain.zakiuddin/viz/SyscoFoodserviceSalesAnalyticsBigQueryStarSchemaTableau/SalesPerformanceDeepDive) |

---

## Architecture
```
Raw CSVs → BigQuery staging → Star Schema → Analytics Views → Tableau
```

**Tables built:**
- `fact_sales` — central transaction table. Revenue derived manually (source field was all zeros)
- `dim_customer` — customer tiers: Platinum/Gold/Silver/Bronze via PERCENTILE_CONT
- `dim_product` — product name, category
- `dim_category` — category name, specialty flag (rebuilt — source was all FALSE)
- `dim_rep` — rep name, hire date, years tenure
- `dim_date` — full 2018 date spine via GENERATE_DATE_ARRAY

---

## SQL Files

| File | Description |
| `sql/01_star_schema.sql` | fact_sales + all 5 dimension tables |
| `sql/02_analytics_views.sql` | Reporting views with window functions for Tableau |
| `sql/03_data_quality_fixes.sql` | Source data issues found and how each was fixed |

---

## Key SQL Techniques

- `DATE_TRUNC(date, MONTH)` — monthly reporting grain
- `DENSE_RANK() OVER (PARTITION BY sales_month ORDER BY revenue DESC)` — rep ranking per month
- `LAG(total_revenue) OVER (ORDER BY month)` — month over month growth
- `PERCENTILE_CONT(revenue, 0.75) OVER()` — dynamic customer tier thresholds
- `SUM(revenue) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` — running total
- `SAFE_DIVIDE()` — avoids division by zero errors
- CTEs to pre-aggregate before window functions 

---

## Data Quality Issues Solved

1. **Revenue field all zeros** — derived manually: `quantity * price * (1 - discount_rate)`
2. **Specialty flag all FALSE** — rebuilt with CASE logic: Meat, Seafood, Dairy = specialty
3. **HireDate as TIMESTAMP** — wrapped in `DATE()` cast before `DATE_DIFF`
4. **Nested window functions** — pre-aggregated in CTEs (BigQuery doesn't allow nesting)

---

## Key Business Insights

- Platinum customers (top 25%) drive **44% of total revenue** — Pareto pattern confirmed
- Specialty revenue = **$116.4M** — 27% of total $428.5M
- March was the only growth month at **+10.7%** MoM
- Holly Collins is the **#1 rep**  performance gap is tight across top reps
- Top 20 customers are well distributed  no dangerous account concentration

---

*Dataset: 6.7M row sales dataset reframed as a foodservice distribution environment*  
*Tools: BigQuery (GCP) · SQL · Tableau Public*
