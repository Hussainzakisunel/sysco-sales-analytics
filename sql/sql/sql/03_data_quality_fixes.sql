-- data quality issues found in source data and how they were fixed
-- Hussain Zakiuddin

-- ISSUE 1: TotalPrice column was all zeros
-- couldn't use source revenue field at all
-- fix: derived revenue manually from components
-- revenue = quantity * unit_price * (1 - discount_rate)
-- this gives net revenue after discount -- actual money received

SELECT
  SalesID,
  Quantity,
  Price,
  Discount,
  TotalPrice, -- this was all zeros in source
  Quantity * Price * (1 - Discount) AS derived_revenue -- what we actually used
FROM sysco_portfolio.raw_sales s
LEFT JOIN sysco_portfolio.raw_products p ON s.ProductID = p.ProductID
LIMIT 10;


-- ISSUE 2: specialty column in source was all FALSE
-- zero analytical value, completely unusable
-- fix: rebuilt specialty_flag using category business logic
-- meat, seafood, dairy = specialty in foodservice because:
--   - perishable / cold chain logistics required
--   - higher margin than dry goods
--   - requires specialized sales knowledge
-- mirrors sysco's actual specialty division structure

SELECT
  CategoryName,
  CASE
    WHEN LOWER(CategoryName) IN ('meat', 'seafood', 'dairy') THEN TRUE
    ELSE FALSE
  END AS specialty_flag_rebuilt
FROM sysco_portfolio.raw_categories
ORDER BY specialty_flag_rebuilt DESC;


-- ISSUE 3: HireDate in raw_employees comes in as TIMESTAMP not DATE
-- DATE_DIFF fails if you pass a TIMESTAMP directly
-- fix: wrap in DATE() cast before using in DATE_DIFF

-- this fails:
-- DATE_DIFF(CURRENT_DATE(), HireDate, YEAR)

-- this works:
SELECT
  EmployeeID,
  HireDate, -- TIMESTAMP
  DATE(HireDate), -- cast to DATE first
  DATE_DIFF(CURRENT_DATE(), DATE(HireDate), YEAR) AS years_tenure
FROM sysco_portfolio.raw_employees
LIMIT 5;


-- ISSUE 4: customer names inconsistent across source tables
-- e.g. 'Sysco Corp', 'SYSCO Corporation', 'sysco'
-- quick SQL standardization pass:

SELECT
  CustomerID,
  TRIM(LOWER(FirstName)) AS first_name_clean,
  TRIM(LOWER(LastName))  AS last_name_clean
FROM sysco_portfolio.raw_customers
LIMIT 10;

-- for production-grade deduplication would use python fuzzy matching
-- (fuzzywuzzy / rapidfuzz) to cluster similar names and build a canonical mapping table


-- ISSUE 5: bigquery does not allow nested window functions
-- this pattern FAILS in bigquery:
-- DENSE_RANK() OVER (ORDER BY SUM(revenue) OVER (PARTITION BY rep_id) DESC)

-- fix: pre-aggregate in a CTE first, then rank off the result
-- see 02_analytics_views.sql for full implementation
-- pattern:
WITH rep_totals AS (
  SELECT
    rep_id,
    SUM(revenue) AS rep_revenue
  FROM sysco_portfolio.fact_sales
  GROUP BY rep_id
)
SELECT
  rep_id,
  rep_revenue,
  DENSE_RANK() OVER (ORDER BY rep_revenue DESC) AS rank
FROM rep_totals;
