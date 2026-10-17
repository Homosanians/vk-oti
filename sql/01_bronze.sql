DROP TABLE IF EXISTS bronze.lakehouse.raw_nation;
DROP TABLE IF EXISTS bronze.lakehouse.raw_customer;
DROP TABLE IF EXISTS bronze.lakehouse.raw_orders;
DROP TABLE IF EXISTS bronze.lakehouse.raw_lineitem;

CREATE TABLE bronze.lakehouse.raw_nation
WITH (format = '${BRONZE_FORMAT}')
AS SELECT n.*,
          'tpch.sf${SF}.nation' AS _source,
          CAST(current_timestamp AS timestamp(6) with time zone) AS _loaded_at
FROM tpch.sf${SF}.nation AS n;

CREATE TABLE bronze.lakehouse.raw_customer
WITH (format = '${BRONZE_FORMAT}')
AS SELECT c.*,
          'tpch.sf${SF}.customer' AS _source,
          CAST(current_timestamp AS timestamp(6) with time zone) AS _loaded_at
FROM tpch.sf${SF}.customer AS c;

CREATE TABLE bronze.lakehouse.raw_orders
WITH (format = '${BRONZE_FORMAT}')
AS SELECT o.*,
          'tpch.sf${SF}.orders' AS _source,
          CAST(current_timestamp AS timestamp(6) with time zone) AS _loaded_at
FROM tpch.sf${SF}.orders AS o;

CREATE TABLE bronze.lakehouse.raw_lineitem
WITH (format = '${BRONZE_FORMAT}')
AS SELECT l.*,
          'tpch.sf${SF}.lineitem' AS _source,
          CAST(current_timestamp AS timestamp(6) with time zone) AS _loaded_at
FROM tpch.sf${SF}.lineitem AS l;
