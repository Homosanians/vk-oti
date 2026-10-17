import sys
sys.path.insert(0, "/opt/bench")
from pyspark.sql import SparkSession
from queries import Q10

spark = SparkSession.builder.appName("explain-q10").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.sql(Q10.format(p="bronze.src_sf10.raw_"))
df.collect()  # AQE финализирует план только после выполнения
plan = df._jdf.queryExecution().executedPlan().toString()
for kw in ["SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin", "BatchScan", "Exchange"]:
    print(f"{kw}: {plan.count(kw)}")
print(plan[:3000])
