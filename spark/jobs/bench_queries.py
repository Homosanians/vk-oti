import argparse
import json
import statistics
import sys
import time

sys.path.insert(0, "/opt/bench")
from pyspark.sql import SparkSession
from queries import QUERIES

ap = argparse.ArgumentParser()
ap.add_argument("--prefix", required=True)
ap.add_argument("--runs", type=int, default=3)
ap.add_argument("--cache", action="store_true", help="CACHE TABLE перед тёплыми прогонами")
a = ap.parse_args()

spark = SparkSession.builder.appName("oti-bench").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def run(sql):
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    return time.perf_counter() - t0, rows


res = {}
for name, sql in QUERIES.items():
    sql = sql.format(p="bronze." + a.prefix)
    cold, rows = run(sql)
    warm = [run(sql)[0] for _ in range(a.runs)]
    res[name] = {"cold_s": round(cold, 3), "warm_median_s": round(statistics.median(warm), 3),
                 "warm_min_s": round(min(warm), 3), "result_rows": len(rows)}

if a.cache:
    for t in ["customer", "orders", "lineitem"]:
        spark.sql(f"CACHE TABLE bronze.{a.prefix}{t}")
    for name, sql in QUERIES.items():
        sql = sql.format(p="bronze." + a.prefix)
        warm = [run(sql)[0] for _ in range(a.runs)]
        res[name]["cached_median_s"] = round(statistics.median(warm), 3)

spark.stop()
print(json.dumps(res))
