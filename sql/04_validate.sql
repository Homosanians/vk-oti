WITH reference AS (
  SELECT c.custkey,
         round(sum(l.extendedprice * (1 - l.discount)), 2) AS revenue
  FROM tpch.sf${SF}.customer AS c
  JOIN tpch.sf${SF}.orders   AS o ON c.custkey = o.custkey
  JOIN tpch.sf${SF}.lineitem AS l ON l.orderkey = o.orderkey
  JOIN tpch.sf${SF}.nation   AS n ON c.nationkey = n.nationkey
  WHERE o.orderdate >= DATE '1993-10-01'
    AND o.orderdate <  DATE '1994-01-01'
    AND l.returnflag = 'R'
  GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
  ORDER BY revenue DESC
  LIMIT 20
),
gold AS (
  SELECT custkey, round(CAST(revenue AS double), 2) AS revenue
  FROM gold.lakehouse.q10_top20_returned_customers
)
SELECT 'only_in_reference' AS side, * FROM (SELECT * FROM reference EXCEPT SELECT * FROM gold)
UNION ALL
SELECT 'only_in_gold', * FROM (SELECT * FROM gold EXCEPT SELECT * FROM reference);
