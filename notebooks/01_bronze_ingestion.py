# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC reads raw csv from volume and writes as delta managed tables

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %run ../includes/schema

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## ingest csv to bronze delta tables

# COMMAND ----------

for csv_file, table_name in CSV_TO_TABLE.items():
    df = (
        spark.read
        .option("header", "true")
        .schema(schemas[table_name])
        .csv(f"{RAW_PATH}/{csv_file}")
    )

    df = df.withColumn("_ingestion_timestamp", current_timestamp())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"workspace.olist_bronze.{table_name}")
    )

    count = spark.table(f"workspace.olist_bronze.{table_name}").count()
    print(f"{table_name:25s} -> {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate bronze tables

# COMMAND ----------

for table_name in BRONZE_TABLES:
    df = spark.table(f"workspace.olist_bronze.{table_name}")
    print(f"\n-- {table_name} --")
    print(f"rows: {df.count():,}")
    print(f"column: {len(df.columns)}")
    df.printSchema()