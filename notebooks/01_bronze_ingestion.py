# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC reads raw csv from volume and writes as delta managed tables

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema definitions

# COMMAND ----------

schemas = {
    "orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ]),
    "order_items": StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("shipping_limit_date", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
    ]),
    "order_payments": StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ]),
    "order_reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("review_score", IntegerType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", StringType(), True),
        StructField("review_answer_timestamp", StringType(), True),
    ]),
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), False),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType(), False),
        StructField("seller_zip_code_prefix", StringType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
    ]),
    "products": StructType([
        StructField("product_id", StringType(), False),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", IntegerType(), True),
        StructField("product_description_lenght", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", IntegerType(), True),
        StructField("product_height_cm", IntegerType(), True),
        StructField("product_width_cm", IntegerType(), True),
    ]),
    "category_translation": StructType([
        StructField("product_category_name", StringType(), False),
        StructField("product_category_name_english", StringType(), True),
    ]),
}

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