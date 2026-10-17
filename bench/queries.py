"""Запросы бенчмарка. {p}: префикс таблиц ячейки, например bronze.bench.parquet_zstd_."""

Q10 = """
SELECT c.custkey, c.name, sum(l.extendedprice * (1 - l.discount)) AS revenue,
       c.acctbal, n.name AS nation, c.address, c.phone, c.comment
FROM {p}customer AS c
JOIN {p}orders   AS o ON c.custkey = o.custkey
JOIN {p}lineitem AS l ON l.orderkey = o.orderkey
JOIN bronze.lakehouse.raw_nation AS n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20
"""

AGG_SCAN = """
SELECT returnflag, linestatus, sum(quantity) AS sum_qty, sum(extendedprice) AS sum_base,
       sum(extendedprice * (1 - discount)) AS sum_disc, avg(discount) AS avg_disc, count(*) AS cnt
FROM {p}lineitem
WHERE shipdate <= DATE '1998-09-02'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus
"""

AGG_NARROW = """
SELECT sum(extendedprice) AS rev
FROM {p}lineitem
WHERE shipdate BETWEEN DATE '1995-03-01' AND DATE '1995-03-31'
"""

QUERIES = {"q10": Q10, "agg_scan": AGG_SCAN, "agg_narrow": AGG_NARROW}
