-- ============================================================
-- SYSCO FOODSERVICE SALES ANALYTICS
-- Star Schema — BigQuery
-- Author: Hussain Zakiuddin
-- ============================================================

-- FACT TABLE
CREATE OR REPLACE TABLE sysco_portfolio.fact_sales AS
SELECT
  s.SalesID                                          AS sales_id,
  s.CustomerID                                       AS customer_id,
  s.ProductID                                        AS product_id,
  s.EmployeeID                                       AS rep_id,
  DATE(s.SaleDate)                                   AS sales_date,
  DATE_TRUNC(DATE(s.SaleDate), MONTH)                AS sales_month,
  s.Quantity                                         AS quantity,
  p.Price                                            AS unit_price,
  s.Discount                                         AS discount_rate,
  s.Quantity * p.Price * (1 - s.Discount)            AS revenue
FROM sysco_portfolio.raw_sales s
LEFT JOIN sysco_portfolio.raw_products p
  ON s.ProductID = p.ProductID;

-- ============================================================
-- DIMENSION: DATE
-- ============================================================
CREATE OR REPLACE TABLE sysco_portfolio.dim_date AS
SELECT
  d                                                  AS date_key,
  FORMAT_DATE('%B', d)                               AS month_name,
  EXTRACT(MONTH FROM d)                              AS month_number,
  EXTRACT(QUARTER FROM d)                            AS quarter,
  EXTRACT(YEAR FROM d)                               AS year,
  FORMAT_DATE('%A', d)                               AS day_of_week,
  CASE WHEN EXTRACT(DAYOFWEEK FROM d) IN (1,7)
       THEN TRUE ELSE FALSE END                      AS is_weekend
FROM UNNEST(
  GENERATE_DATE_ARRAY('2018-01-01', '2018-12-31', INTERVAL 1 DAY)
) AS d;

-- ============================================================
-- DIMENSION: PRODUCT
-- ============================================================
CREATE OR REPLACE TABLE sysco_portfolio.dim_product AS
SELECT
  p.ProductID                                        AS product_id,
  p.ProductName                                      AS product_name,
  p.CategoryID                                       AS category_id,
  p.Price                                            AS unit_price
FROM sysco_portfolio.raw_products p;

-- ============================================================
-- DIMENSION: CATEGORY
-- ============================================================
CREATE OR REPLACE TABLE sysco_portfolio.dim_category AS
SELECT
  c.CategoryID                                       AS category_id,
  c.CategoryName                                     AS category_name,
  CASE
    WHEN LOWER(c.CategoryName) IN ('meat','seafood','dairy')
    THEN TRUE ELSE FALSE
  END                                                AS specialty_flag
FROM sysco_portfolio.raw_categories c;

-- ============================================================
-- DIMENSION: CUSTOMER
-- ============================================================
CREATE OR REPLACE TABLE sysco_portfolio.dim_customer AS
WITH customer_revenue AS (
  SELECT
    customer_id,
    SUM(revenue) AS total_revenue
  FROM sysco_portfolio.fact_sales
  GROUP BY 1
),
percentiles AS (
  SELECT
    PERCENTILE_CONT(total_revenue, 0.75) OVER() AS p75,
    PERCENTILE_CONT(total_revenue, 0.50) OVER() AS p50,
    PERCENTILE_CONT(total_revenue, 0.25) OVER() AS p25,
    customer_id,
    total_revenue
  FROM customer_revenue
)
SELECT
  c.CustomerID                                       AS customer_id,
  CONCAT(c.FirstName, ' ', c.LastName)               AS customer_name,
  CASE
    WHEN p.total_revenue >= p.p75 THEN 'Platinum'
    WHEN p.total_revenue >= p.p50 THEN 'Gold'
    WHEN p.total_revenue >= p.p25 THEN 'Silver'
    ELSE 'Bronze'
  END                                                AS customer_segment,
  p.total_revenue
FROM sysco_portfolio.raw_customers c
JOIN percentiles p
  ON c.CustomerID = p.customer_id;

-- ============================================================
-- DIMENSION: SALES REP
-- ============================================================
CREATE OR REPLACE TABLE sysco_portfolio.dim_rep AS
SELECT
  e.EmployeeID                                       AS rep_id,
  CONCAT(e.FirstName, ' ', e.LastName)               AS rep_name,
  DATE(e.HireDate)                                   AS hire_date,
  DATE_DIFF(CURRENT_DATE(), DATE(e.HireDate), YEAR)  AS years_tenure
FROM sysco_portfolio.raw_employees e;
