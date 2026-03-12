-- analytics views -- reporting layer on top of the star schema
-- these are what tableau connects to
-- Hussain Zakiuddin

-- main tableau view
-- denormalized, all dims joined, window functions pre-computed
-- note: bigquery doesnt allow nested window functions so aggregations
-- are pre-computed in CTEs before ranking off them
CREATE OR REPLACE VIEW sysco_portfolio.v_tableau_sales AS
WITH base AS (
  SELECT
    fs.sales_id,
    fs.sales_date,
    fs.sales_month,
    fs.quantity,
    fs.unit_price,
    fs.discount_rate,
    fs.revenue,
    dc.customer_id,
    dc.customer_name,
    dc.customer_segment,
    dr.rep_id,
    dr.rep_name,
    dr.years_tenure,
    dp.product_name,
    dcat.category_name,
    dcat.specialty_flag
  FROM sysco_portfolio.fact_sales fs
  LEFT JOIN sysco_portfolio.dim_customer  dc   ON fs.customer_id = dc.customer_id
  LEFT JOIN sysco_portfolio.dim_rep       dr   ON fs.rep_id      = dr.rep_id
  LEFT JOIN sysco_portfolio.dim_product   dp   ON fs.product_id  = dp.product_id
  LEFT JOIN sysco_portfolio.dim_category  dcat ON dp.category_id = dcat.category_id
),
-- pre-aggregating rep monthly revenue before ranking
-- cant do DENSE_RANK(ORDER BY SUM(revenue) OVER(...)) directly in bigquery
rep_monthly AS (
  SELECT
    rep_id,
    sales_month,
    SUM(revenue) AS rep_monthly_revenue
  FROM base
  GROUP BY rep_id, sales_month
),
category_totals AS (
  SELECT
    category_name,
    SUM(revenue) AS category_total_revenue
  FROM base
  GROUP BY category_name
)
SELECT
  b.*,
  rm.rep_monthly_revenue,
  DENSE_RANK() OVER (
    PARTITION BY b.sales_month
    ORDER BY rm.rep_monthly_revenue DESC
  )                                                     AS rep_monthly_rank,
  -- customer lifetime revenue
  SUM(b.revenue) OVER (PARTITION BY b.customer_id)     AS customer_ltv,
  -- customer share of total revenue
  ROUND(SAFE_DIVIDE(
    SUM(b.revenue) OVER (PARTITION BY b.customer_id),
    SUM(b.revenue) OVER ()
  ) * 100, 4)                                           AS customer_revenue_pct,
  -- running total
  SUM(b.revenue) OVER (
    ORDER BY b.sales_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                                     AS running_total_revenue,
  ct.category_total_revenue
FROM base b
LEFT JOIN rep_monthly  rm ON b.rep_id = rm.rep_id AND b.sales_month = rm.sales_month
LEFT JOIN category_totals ct ON b.category_name = ct.category_name;


-- customer concentration / pareto view
CREATE OR REPLACE VIEW sysco_portfolio.v_customer_concentration AS
WITH customer_totals AS (
  SELECT
    customer_id,
    customer_name,
    customer_segment,
    SUM(revenue) AS total_revenue
  FROM sysco_portfolio.v_tableau_sales
  GROUP BY customer_id, customer_name, customer_segment
),
with_grand_total AS (
  SELECT
    customer_id,
    customer_name,
    customer_segment,
    total_revenue,
    -- pre-computing grand total as plain column before window function
    SUM(total_revenue) OVER() AS grand_total_revenue
  FROM customer_totals
)
SELECT
  customer_id,
  customer_name,
  customer_segment,
  total_revenue,
  ROUND(SAFE_DIVIDE(total_revenue, grand_total_revenue) * 100, 2) AS revenue_share_pct,
  -- cumulative revenue % for pareto curve
  ROUND(SUM(SAFE_DIVIDE(total_revenue, grand_total_revenue) * 100) OVER (
    ORDER BY total_revenue DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ), 2)                                                AS cumulative_revenue_pct,
  DENSE_RANK() OVER (ORDER BY total_revenue DESC)      AS customer_rank
FROM with_grand_total
ORDER BY total_revenue DESC;


-- rep performance view -- monthly leaderboard with specialty mix
CREATE OR REPLACE VIEW sysco_portfolio.v_rep_performance AS
WITH rep_month AS (
  SELECT
    rep_id,
    rep_name,
    sales_month,
    SUM(revenue)                                        AS total_revenue,
    SUM(CASE WHEN specialty_flag THEN revenue ELSE 0 END) AS specialty_revenue,
    COUNT(DISTINCT sales_id)                            AS order_count
  FROM sysco_portfolio.v_tableau_sales
  GROUP BY rep_id, rep_name, sales_month
)
SELECT
  rep_id,
  rep_name,
  sales_month,
  total_revenue,
  specialty_revenue,
  order_count,
  ROUND(SAFE_DIVIDE(specialty_revenue, total_revenue) * 100, 2) AS specialty_mix_pct,
  DENSE_RANK() OVER (
    PARTITION BY sales_month
    ORDER BY total_revenue DESC
  )                                                     AS monthly_rank
FROM rep_month
ORDER BY sales_month, monthly_rank;


-- monthly kpi summary with mom growth
CREATE OR REPLACE TABLE sysco_portfolio.kpi_monthly_summary AS
WITH monthly AS (
  SELECT
    sales_month,
    SUM(revenue)                                          AS total_revenue,
    SUM(CASE WHEN specialty_flag THEN revenue ELSE 0 END) AS specialty_revenue,
    COUNT(DISTINCT sales_id)                              AS total_orders,
    COUNT(DISTINCT customer_id)                           AS active_customers
  FROM sysco_portfolio.v_tableau_sales
  GROUP BY sales_month
)
SELECT
  sales_month,
  total_revenue,
  specialty_revenue,
  total_orders,
  active_customers,
  ROUND(SAFE_DIVIDE(specialty_revenue, total_revenue) * 100, 2) AS specialty_mix_pct,
  LAG(total_revenue) OVER (ORDER BY sales_month)                AS prior_month_revenue,
  ROUND(SAFE_DIVIDE(
    total_revenue - LAG(total_revenue) OVER (ORDER BY sales_month),
    LAG(total_revenue) OVER (ORDER BY sales_month)
  ) * 100, 2)                                                   AS mom_revenue_growth_pct
FROM monthly
ORDER BY sales_month;
