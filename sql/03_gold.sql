DROP TABLE IF EXISTS gold.lakehouse.returned_revenue_by_customer_quarter;
DROP TABLE IF EXISTS gold.lakehouse.q10_top20_returned_customers;

CREATE TABLE gold.lakehouse.returned_revenue_by_customer_quarter
WITH (format = 'PARQUET', partitioning = ARRAY['year(order_quarter)'])
AS
SELECT
  f.custkey,
  CAST(date_trunc('quarter', f.orderdate) AS date) AS order_quarter,
  sum(f.net_revenue)                               AS returned_revenue,
  count(*)                                         AS returned_lines
FROM silver.lakehouse.fact_order_line AS f
WHERE f.returnflag = 'R'
GROUP BY f.custkey, date_trunc('quarter', f.orderdate);

CREATE TABLE gold.lakehouse.q10_top20_returned_customers
WITH (format = 'PARQUET')
AS
SELECT
  c.custkey,
  c.name,
  CAST(sum(f.net_revenue) AS decimal(18, 2)) AS revenue,
  c.acctbal,
  c.nation_name AS nation,
  c.address,
  c.phone,
  c.comment
FROM silver.lakehouse.fact_order_line AS f
JOIN silver.lakehouse.dim_customer    AS c ON c.custkey = f.custkey
WHERE f.orderdate >= DATE '1993-10-01'
  AND f.orderdate <  DATE '1994-01-01'
  AND f.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, c.nation_name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20;
