DROP TABLE IF EXISTS silver.lakehouse.dim_customer;
DROP TABLE IF EXISTS silver.lakehouse.fact_order_line;
DROP TABLE IF EXISTS silver.lakehouse.dq_report;

CREATE TABLE silver.lakehouse.dim_customer
WITH (format = 'PARQUET')
AS
WITH dedup AS (
  SELECT c.*, row_number() OVER (PARTITION BY c.custkey ORDER BY c._loaded_at DESC) AS rn
  FROM bronze.lakehouse.raw_customer AS c
  WHERE c.custkey IS NOT NULL
)
SELECT
  d.custkey,
  trim(d.name)                                   AS name,
  trim(d.address)                                AS address,
  trim(d.phone)                                  AS phone,
  CAST(substr(trim(d.phone), 1, 2) AS integer)   AS phone_country_code,
  CAST(d.acctbal AS decimal(12, 2))              AS acctbal,
  upper(trim(d.mktsegment))                      AS mktsegment,
  trim(d.comment)                                AS comment,
  d.nationkey,
  upper(trim(n.name))                            AS nation_name,
  n.regionkey,
  d._loaded_at                                   AS bronze_loaded_at
FROM dedup AS d
LEFT JOIN bronze.lakehouse.raw_nation AS n ON n.nationkey = d.nationkey
WHERE d.rn = 1;

-- Факт грузится INSERT'ами по годам: один CTAS на sf50 не помещался в память узла.
CREATE TABLE silver.lakehouse.fact_order_line
WITH (
  format       = 'PARQUET',
  partitioning = ARRAY['month(orderdate)'],
  sorted_by    = ARRAY['returnflag', 'custkey']
)
AS
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
WITH NO DATA;

INSERT INTO silver.lakehouse.fact_order_line
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
  AND o.orderdate >= DATE '1992-01-01' AND o.orderdate < DATE '1993-01-01';

INSERT INTO silver.lakehouse.fact_order_line
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
  AND o.orderdate >= DATE '1993-01-01' AND o.orderdate < DATE '1994-01-01';

INSERT INTO silver.lakehouse.fact_order_line
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
  AND o.orderdate >= DATE '1994-01-01' AND o.orderdate < DATE '1995-01-01';

INSERT INTO silver.lakehouse.fact_order_line
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
  AND o.orderdate >= DATE '1995-01-01' AND o.orderdate < DATE '1996-01-01';

INSERT INTO silver.lakehouse.fact_order_line
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
  AND o.orderdate >= DATE '1996-01-01' AND o.orderdate < DATE '1997-01-01';

INSERT INTO silver.lakehouse.fact_order_line
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
  AND o.orderdate >= DATE '1997-01-01' AND o.orderdate < DATE '1998-01-01';

INSERT INTO silver.lakehouse.fact_order_line
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  o.orderstatus,
  upper(l.returnflag)                                       AS returnflag,
  upper(l.linestatus)                                       AS linestatus,
  CAST(l.quantity      AS decimal(12, 2))                   AS quantity,
  CAST(l.extendedprice AS decimal(12, 2))                   AS extendedprice,
  CAST(l.discount      AS decimal(4, 2))                    AS discount,
  CAST(l.tax           AS decimal(4, 2))                    AS tax,
  CAST(CAST(l.extendedprice AS decimal(12, 2))
       * (1 - CAST(l.discount AS decimal(4, 2))) AS decimal(18, 4)) AS net_revenue,
  l.shipdate,
  l.receiptdate
FROM bronze.lakehouse.raw_lineitem AS l
JOIN bronze.lakehouse.raw_orders   AS o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND l.discount BETWEEN 0 AND 1
  AND l.extendedprice >= 0
  AND o.orderdate >= DATE '1998-01-01' AND o.orderdate < DATE '1999-01-01';

CREATE TABLE silver.lakehouse.dq_report WITH (format = 'PARQUET') AS
SELECT 'bronze.raw_customer: дубли custkey' AS rule,
       count(*) - count(DISTINCT custkey) AS failed_rows
FROM bronze.lakehouse.raw_customer
UNION ALL
SELECT 'bronze.raw_lineitem: строки без заказа в orders', count(*)
FROM bronze.lakehouse.raw_lineitem AS l
LEFT JOIN bronze.lakehouse.raw_orders AS o ON o.orderkey = l.orderkey
WHERE o.orderkey IS NULL
UNION ALL
SELECT 'bronze.raw_lineitem: discount вне [0,1] или цена < 0', count(*)
FROM bronze.lakehouse.raw_lineitem
WHERE NOT (discount BETWEEN 0 AND 1) OR extendedprice < 0
UNION ALL
SELECT 'silver.dim_customer: страна не найдена', count(*)
FROM silver.lakehouse.dim_customer WHERE nation_name IS NULL
UNION ALL
SELECT 'silver.dim_customer: код телефона != nationkey+10 (правило TPC-H)', count(*)
FROM silver.lakehouse.dim_customer WHERE phone_country_code <> nationkey + 10
UNION ALL
SELECT 'silver.fact_order_line: потеряно строк относительно bronze.raw_lineitem',
       (SELECT count(*) FROM bronze.lakehouse.raw_lineitem) - count(*)
FROM silver.lakehouse.fact_order_line;
