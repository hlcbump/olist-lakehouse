# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - delta lake features demo
# MAGIC demonstrate merge, time travel, schema enforcement, optimize, z-ordering, vacuum

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 - merge upsert incremental

# COMMAND ----------

# MAGIC %md
# MAGIC ### load batch 1 (orders before 2018-08-01)

# COMMAND ----------

df_all = spark.table("workspace.olist_silver.orders")

batch1 = df_all.filter(col("order_purchase_timestamp") < to_timestamp(lit("2018-08-01")))
batch2 = df_all.filter(col("order_purchase_timestamp") >= to_timestamp(lit("2018-08-01")))

print(f"Batch 1: {batch1.count():,} rows")
print(f"Batch 2: {batch2.count():,} rows")

# COMMAND ----------

batch1.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.orders_merge_demo")
print(f"After batch 1: {spark.table('workspace.olist_silver.orders_merge_demo').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### merge batch 2 into the table

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "workspace.olist_silver.orders_merge_demo")

# pego a tabela atual 'orders_merge_demo' e comparo com batch2, comparo por order id
# se order_id existe, faz update, quando não existe faz insert
delta_table.alias("target") \
    .merge(batch2.alias("source"), "target.order_id = source.order_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print(f"After MERGE: {spark.table('workspace.olist_silver.orders_merge_demo').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 - time travel

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY workspace.olist_silver.orders_merge_demo"))

# COMMAND ----------

# pegando a versão da tabela com batch 1
df_v0 = spark.read.option("versionAsOf", 0).table("workspace.olist_silver.orders_merge_demo")

# pegando a versão da tabela com batch 1 + batch 2
df_v1 = spark.read.option("versionAsOf", 1).table("workspace.olist_silver.orders_merge_demo")

print(f"version 0 (batch 1 only): {df_v0.count():,} rows")
print(f"version 1 (after merge): {df_v1.count():,} rows")

new_records = df_v1.subtract(df_v0)
print(f"new/changed recods:   {new_records.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 - schema enforcement

# COMMAND ----------

# criando uma tabela demo (cópia da olist_silver.orders)
# customer_id é string
spark.sql("""CREATE OR REPLACE TABLE workspace.olist_silver.orders_schema_demo
            AS SELECT * FROM workspace.olist_silver.orders limit 1000
""")

# cria um df com customer_id como integer
bad_data = spark.createDataFrame(
    [("order_999", 12345, "invalid_status")],
    ["order_id", "customer_id", "order_status"]
)

# tentar inserir o record na tabela com customer_id com tipo errado
try:
    bad_data.write.format("delta").mode("append").saveAsTable("workspace.olist_silver.orders_schema_demo")
except Exception as e:
    print(f"schema enforcement blocked the write: \n{e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 - schema evolution

# COMMAND ----------

# adicionando uma nova coluna 'processing_region'
spark.sql("ALTER TABLE workspace.olist_silver.orders_schema_demo ADD COLUMNS (processing_region STRING)")
display(spark.table("workspace.olist_silver.orders_schema_demo").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 - optimize + z-ordering

# COMMAND ----------

print("before optimize:")
display(spark.sql("DESCRIBE DETAIL workspace.olist_silver.orders_merge_demo").select("numFiles", "sizeInBytes"))

# COMMAND ----------

# compactando e reorganizando os dados em 'orders_merge_demo' pelo order_id
spark.sql("OPTIMIZE workspace.olist_silver.orders_merge_demo ZORDER BY (order_id)")

# COMMAND ----------

print("after optimize:")
display(spark.sql("DESCRIBE DETAIL workspace.olist_silver.orders_merge_demo").select("numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 - vacuum

# COMMAND ----------

spark.sql("VACUUM workspace.olist_silver.orders_merge_demo RETAIN 168 HOURS")
print("vacuum complete - old files removed")