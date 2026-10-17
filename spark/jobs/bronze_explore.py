from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("bronze-explore").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def show(title, df, n=10):
    print(f"\n### {title}")
    df.show(n, truncate=60)


show("Каталог bronze: пространства имён", spark.sql("SHOW NAMESPACES IN bronze"))
show("Таблицы Bronze", spark.sql("SHOW TABLES IN bronze.lakehouse"))
show("Схема raw_lineitem (её записал Trino, читает Spark)", spark.sql("DESCRIBE TABLE bronze.lakehouse.raw_lineitem"), 30)

print("\n### Число строк в Bronze")
for t in ["raw_nation", "raw_customer", "raw_orders", "raw_lineitem"]:
    print(f"{t:<14} {spark.table(f'bronze.lakehouse.{t}').count():>12,}")

show("Метаданные Iceberg: снапшоты raw_lineitem",
     spark.sql("SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added_records, "
               "summary['added-files-size'] AS added_bytes FROM bronze.lakehouse.raw_lineitem.snapshots"))
show("Метаданные Iceberg: файлы raw_lineitem (формат, строки, размер)",
     spark.sql("SELECT file_format, count(*) AS files, sum(record_count) AS rows, "
               "round(sum(file_size_in_bytes)/1024/1024, 1) AS mb FROM bronze.lakehouse.raw_lineitem.files "
               "GROUP BY file_format"))

c = spark.table("bronze.lakehouse.raw_customer")
o = spark.table("bronze.lakehouse.raw_orders")
l = spark.table("bronze.lakehouse.raw_lineitem")
n = spark.table("bronze.lakehouse.raw_nation")
q10 = (c.join(o, "custkey")
        .join(l, "orderkey")
        .join(n.select("nationkey", F.col("name").alias("nation")), "nationkey")
        .where((F.col("orderdate") >= "1993-10-01") & (F.col("orderdate") < "1994-01-01")
               & (F.col("returnflag") == "R"))
        .groupBy("custkey", c["name"], "acctbal", "nation", "address", "phone", c["comment"])
        .agg(F.round(F.sum(F.col("extendedprice") * (1 - F.col("discount"))), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(20))
show("Q10 на Spark DataFrame API по Bronze (топ-5 из 20)", q10.select("custkey", "name", "revenue", "nation"), 5)
show("План: Iceberg-скан с pushdown фильтров (фрагмент)",
     spark.createDataFrame([(l.where(F.col("returnflag") == "R").select("orderkey")
                             ._jdf.queryExecution().executedPlan().toString()[:600],)], ["plan"]), 1)
spark.stop()
