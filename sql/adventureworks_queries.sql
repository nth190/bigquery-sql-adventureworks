-- ============================================================
-- BigQuery SQL Analytics — AdventureWorks (Public Dataset)
-- Author: Hieu Nguyen
-- Dialect: BigQuery Standard SQL
-- Dataset: adventureworks2019
-- ============================================================


-- ============================================================
-- Query 01
-- Calc Quantity of items, Sales value & Order quantity by Subcategory (Last 12 Months)
-- Output: Month (Period), Subcategory, qty_item, total_sales, order_cnt
-- Notes:
--   - Uses the latest ModifiedDate in the dataset as the anchor date
--   - Keeps all subcategories (LEFT JOIN)
-- ============================================================

WITH max_date_cte AS (
  SELECT MAX(DATE(ModifiedDate)) AS max_date
  FROM `adventureworks2019.Sales.SalesOrderDetail`
)
SELECT
  FORMAT_DATE('%b %Y', DATE(a.ModifiedDate)) AS period,
  c.Name AS subcategory_name,
  SUM(a.OrderQty) AS qty_item,
  SUM(a.LineTotal) AS total_sales,
  COUNT(DISTINCT a.SalesOrderID) AS order_cnt
FROM `adventureworks2019.Sales.SalesOrderDetail` AS a
CROSS JOIN max_date_cte
LEFT JOIN `adventureworks2019.Production.Product` AS b
  ON a.ProductID = b.ProductID
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS c
  ON SAFE_CAST(b.ProductSubcategoryID AS INT64) = c.ProductSubcategoryID
WHERE DATE(a.ModifiedDate)
  BETWEEN DATE_SUB(max_date, INTERVAL 12 MONTH) AND max_date
GROUP BY period, subcategory_name
ORDER BY subcategory_name, period;


-- ============================================================
-- Query 02
-- Top 3 Subcategories by YoY growth (OrderQty), based on annual totals
-- Output: Subcategory, year, qty_item, prv_qty, yoy_growth_percent
-- Notes:
--   - Uses DENSE_RANK to keep all ties in Top 3
--   - yoy_growth_percent = (qty - prev_qty) / prev_qty * 100
-- ============================================================

WITH sales_by_subcat_year AS (
  SELECT
    c.Name AS subcategory_name,
    EXTRACT(YEAR FROM a.ModifiedDate) AS yr,
    SUM(a.OrderQty) AS qty_item
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS a
  LEFT JOIN `adventureworks2019.Production.Product` AS b
    ON a.ProductID = b.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS c
    ON SAFE_CAST(b.ProductSubcategoryID AS INT64) = c.ProductSubcategoryID
  WHERE c.Name IS NOT NULL
  GROUP BY subcategory_name, yr
),
growth_base AS (
  SELECT
    subcategory_name,
    yr,
    qty_item,
    LAG(qty_item) OVER (PARTITION BY subcategory_name ORDER BY yr) AS prv_qty
  FROM sales_by_subcat_year
),
growth_calc AS (
  SELECT
    subcategory_name,
    yr,
    qty_item,
    prv_qty,
    ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(qty_item - prv_qty, prv_qty), 100), 2) AS yoy_growth_percent
  FROM growth_base
  WHERE prv_qty IS NOT NULL
),
ranked AS (
  SELECT
    *,
    DENSE_RANK() OVER (ORDER BY yoy_growth_percent DESC) AS rnk
  FROM growth_calc
)
SELECT
  subcategory_name,
  yr,
  qty_item,
  prv_qty,
  yoy_growth_percent
FROM ranked
WHERE rnk <= 3
ORDER BY yoy_growth_percent DESC, subcategory_name, yr;


-- ============================================================
-- Query 03
-- Top 3 TerritoryID with biggest OrderQty of every year (no rank gaps on ties)
-- Output: year, TerritoryID, order_qty, rank
-- Notes:
--   - Uses DENSE_RANK per year to keep ties
-- ============================================================

WITH yearly_territory AS (
  SELECT
    EXTRACT(YEAR FROM d.ModifiedDate) AS yr,
    h.TerritoryID,
    SUM(d.OrderQty) AS order_qty
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS d
  INNER JOIN `adventureworks2019.Sales.SalesOrderHeader` AS h
    ON d.SalesOrderID = h.SalesOrderID
  GROUP BY yr, TerritoryID
),
ranked AS (
  SELECT
    yr,
    TerritoryID,
    order_qty,
    DENSE_RANK() OVER (PARTITION BY yr ORDER BY order_qty DESC) AS rnk
  FROM yearly_territory
)
SELECT
  yr,
  TerritoryID,
  order_qty,
  rnk
FROM ranked
WHERE rnk <= 3
ORDER BY yr DESC, rnk ASC, TerritoryID;


-- ============================================================
-- Query 04
-- Seasonal Discount cost by Subcategory and Year
-- Output: year, subcategory, total_discount_cost
-- Notes:
--   - Discount cost = DiscountPct * UnitPrice * OrderQty
-- ============================================================

WITH seasonal_discount_detail AS (
  SELECT
    EXTRACT(YEAR FROM d.ModifiedDate) AS yr,
    d.ProductID,
    (so.DiscountPct * d.UnitPrice * d.OrderQty) AS discount_cost
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS d
  INNER JOIN `adventureworks2019.Sales.SpecialOffer` AS so
    ON d.SpecialOfferID = so.SpecialOfferID
  WHERE so.Type = 'Seasonal Discount'
),
product_subcat AS (
  SELECT
    p.ProductID,
    sc.Name AS subcategory_name
  FROM `adventureworks2019.Production.Product` AS p
  INNER JOIN `adventureworks2019.Production.ProductSubcategory` AS sc
    ON SAFE_CAST(p.ProductSubcategoryID AS INT64) = sc.ProductSubcategoryID
)
SELECT
  s.yr,
  ps.subcategory_name,
  SUM(s.discount_cost) AS total_discount_cost
FROM seasonal_discount_detail AS s
INNER JOIN product_subcat AS ps
  ON s.ProductID = ps.ProductID
GROUP BY s.yr, ps.subcategory_name
ORDER BY s.yr, ps.subcategory_name;


-- ============================================================
-- Query 05
-- Customer Cohort Retention (2014, Status = 5)
-- Output: cohort_month, month_diff, customer_cnt
-- Notes:
--   - Cohort month = first purchase month in 2014
--   - month_diff = M0, M1, ... based on month distance from cohort
-- ============================================================

WITH orders_2014 AS (
  SELECT
    CustomerID,
    EXTRACT(MONTH FROM ModifiedDate) AS order_month
  FROM `adventureworks2019.Sales.SalesOrderHeader`
  WHERE Status = 5
    AND EXTRACT(YEAR FROM ModifiedDate) = 2014
),
cohort AS (
  SELECT
    CustomerID,
    MIN(order_month) AS cohort_month
  FROM orders_2014
  GROUP BY CustomerID
),
cohort_orders AS (
  SELECT
    o.CustomerID,
    c.cohort_month,
    o.order_month,
    CONCAT('M', CAST(o.order_month - c.cohort_month AS STRING)) AS month_diff
  FROM orders_2014 AS o
  INNER JOIN cohort AS c
    ON o.CustomerID = c.CustomerID
)
SELECT
  cohort_month,
  month_diff,
  COUNT(DISTINCT CustomerID) AS customer_cnt
FROM cohort_orders
GROUP BY cohort_month, month_diff
ORDER BY cohort_month, month_diff;


-- ============================================================
-- Query 06
-- Trend of Stock level & MoM diff % by product in 2011
-- Output: product_name, year, month, stock_qty, stock_prev, mom_diff_percent
-- Notes:
--   - mom_diff_percent uses SAFE_DIVIDE to avoid division by zero
--   - Null growth becomes 0
-- ============================================================

WITH monthly_stock AS (
  SELECT
    p.Name AS product_name,
    EXTRACT(YEAR FROM w.ModifiedDate) AS yr,
    EXTRACT(MONTH FROM w.ModifiedDate) AS mth,
    SUM(w.StockedQty) AS stock_qty
  FROM `adventureworks2019.Production.WorkOrder` AS w
  INNER JOIN `adventureworks2019.Production.Product` AS p
    ON w.ProductID = p.ProductID
  WHERE EXTRACT(YEAR FROM w.ModifiedDate) = 2011
  GROUP BY product_name, yr, mth
),
with_prev AS (
  SELECT
    *,
    LAG(stock_qty) OVER (PARTITION BY product_name ORDER BY mth) AS stock_prev
  FROM monthly_stock
)
SELECT
  product_name,
  yr,
  mth,
  stock_qty,
  stock_prev,
  ROUND(IFNULL(SAFE_MULTIPLY(SAFE_DIVIDE(stock_qty - stock_prev, stock_prev), 100), 0), 1) AS mom_diff_percent
FROM with_prev
ORDER BY product_name, mth DESC;


-- ============================================================
-- Query 07
-- Ratio of Stock / Sales in 2011 by product, by month
-- Output: month, year, product_id, product_name, stock_qty, sales_qty, ratio
-- Notes:
--   - FULL OUTER JOIN keeps months where only stock OR only sales exists
--   - ratio = stock_qty / sales_qty, rounded to 1 decimal
-- ============================================================

WITH stock AS (
  SELECT
    EXTRACT(YEAR FROM w.ModifiedDate) AS yr,
    EXTRACT(MONTH FROM w.ModifiedDate) AS mth,
    p.ProductID,
    p.Name AS product_name,
    SUM(w.StockedQty) AS stock_qty
  FROM `adventureworks2019.Production.WorkOrder` AS w
  INNER JOIN `adventureworks2019.Production.Product` AS p
    ON w.ProductID = p.ProductID
  WHERE EXTRACT(YEAR FROM w.ModifiedDate) = 2011
  GROUP BY yr, mth, ProductID, product_name
),
sales AS (
  SELECT
    EXTRACT(YEAR FROM d.ModifiedDate) AS yr,
    EXTRACT(MONTH FROM d.ModifiedDate) AS mth,
    p.ProductID,
    p.Name AS product_name,
    SUM(d.OrderQty) AS sales_qty
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS d
  INNER JOIN `adventureworks2019.Production.Product` AS p
    ON d.ProductID = p.ProductID
  WHERE EXTRACT(YEAR FROM d.ModifiedDate) = 2011
  GROUP BY yr, mth, ProductID, product_name
)
SELECT
  COALESCE(s.mth, sa.mth) AS mth,
  COALESCE(s.yr, sa.yr) AS yr,
  COALESCE(s.ProductID, sa.ProductID) AS ProductID,
  COALESCE(s.product_name, sa.product_name) AS product_name,
  s.stock_qty,
  sa.sales_qty,
  ROUND(IFNULL(SAFE_DIVIDE(s.stock_qty, sa.sales_qty), 0), 1) AS ratio
FROM stock AS s
FULL OUTER JOIN sales AS sa
  ON s.ProductID = sa.ProductID
 AND s.yr = sa.yr
 AND s.mth = sa.mth
ORDER BY mth DESC, ratio DESC, product_name;


-- ============================================================
-- Query 08
-- Number and total value of purchase orders in 'Pending' status (Status = 1) during 2014
-- Output: year, status, order_count, total_value
-- ============================================================

SELECT
  EXTRACT(YEAR FROM ModifiedDate) AS yr,
  Status,
  COUNT(PurchaseOrderID) AS order_count,
  SUM(TotalDue) AS total_value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
WHERE Status = 1
  AND EXTRACT(YEAR FROM ModifiedDate) = 2014
GROUP BY yr, Status
ORDER BY yr;
