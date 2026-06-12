# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - silver transformations
# MAGIC cleans, types, and enriches bronze data into silver delta tables

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %run ../includes/schema

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, try_to_timestamp, datediff, when, trim, initcap, upper,
    lower, coalesce, lit, current_timestamp, row_number
)
from pyspark.sql.window import Window

# COMMAND ----------

VALID_UFS = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
    "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC",
    "SP","SE","TO"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_orders

# COMMAND ----------

df_orders = spark.table("workspace.olist_bronze.orders")

df_orders = (
    df_orders
    .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
    .withColumn("delivery_days", datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("estimated_vs_actual", datediff("order_estimated_delivery_date", "order_delivered_customer_date"))
    .withColumn("is_late", when(col("estimated_vs_actual") < 0, 1).otherwise(0))
    .drop("_ingestion_timestamp")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_orders.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.orders")
print(f"silver_orders: {spark.table('workspace.olist_silver.orders').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_customers

# COMMAND ----------

df_customers = spark.table("workspace.olist_bronze.customers")

df_customers = (
    df_customers
    .withColumn("customer_city", initcap(trim(col("customer_city"))))
    .withColumn("customer_state", upper(trim(col("customer_state"))))
    .filter(col("customer_state").isin(VALID_UFS))
    .drop("_ingestion_timestamp")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_customers.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.customers")
print(f"silver_customers: {spark.table('workspace.olist_silver.customers').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_products

# COMMAND ----------

df_products = spark.table("workspace.olist_bronze.products")
df_translation = spark.table("workspace.olist_bronze.category_translation")

df_products = (
    df_products
    .join(df_translation, "product_category_name", "left")
    .withColumn("product_category_name_english",
                coalesce(col("product_category_name_english"), lit("uncategorized"))
                )
    .withColumn("product_volume_cm3",
                col("product_length_cm") * col("product_height_cm") * col("product_width_cm")
                )
    .filter(col("product_id").isNotNull())
    .drop("_ingestion_timestamp")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_products.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.products")
print(f"silver_products: {spark.table('workspace.olist_silver.products').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_order_items

# COMMAND ----------

df_items = spark.table("workspace.olist_bronze.order_items")

df_items = (
    df_items
    .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
    .withColumn("total_item_value", col("price") + col("freight_value"))
    .drop("_ingestion_timestamp")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_items.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.order_items")
print(f"silver_order_items: {spark.table('workspace.olist_silver.order_items').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_payments

# COMMAND ----------

df_payments = spark.table("workspace.olist_bronze.order_payments")

df_payments = (
    df_payments
    .withColumn("payment_type", lower(trim(col("payment_type"))))
    .drop("_ingestion_timestamp")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_payments.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.payments")
print(f"silver_payments: {spark.table('workspace.olist_silver.payments').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_reviews

# COMMAND ----------

df_reviews = spark.table("workspace.olist_bronze.order_reviews")

w = Window.partitionBy("order_id").orderBy(col("review_answer_timestamp").desc())

df_reviews = (
    df_reviews
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("review_score", col("review_score").cast("int"))
    .withColumn("review_comment_message",
                coalesce(col("review_comment_message"), lit(""))
                )
    .withColumn("review_creation_date", try_to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", try_to_timestamp("review_answer_timestamp"))
    .drop("_ingestion_timestamp")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_reviews.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.reviews")
print(f"silver_reviews: {spark.table('workspace.olist_silver.reviews').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_sellers

# COMMAND ----------

df_sellers = spark.table("workspace.olist_bronze.sellers")

df_sellers = (
    df_sellers
    .withColumn("seller_city", initcap(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
    .filter(col("seller_state").isin(VALID_UFS))
    .drop("_ingestion_timestamp")
    .withColumn("_ingestion_timestamp", current_timestamp())
)

df_sellers.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_silver.sellers")
print(f"silver_sellers: {spark.table('workspace.olist_silver.sellers').count():,} rows")