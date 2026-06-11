# Databricks notebook source

# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates schemas and defines constants for the Olist Lakehouse pipeline

# COMMAND ----------

# Schema creation
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.olist_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.olist_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.olist_gold")

# COMMAND ----------

# volume for raw csv files
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.olist_bronze.raw")

# COMMAND ----------

# Constants
RAW_PATH = "/Volumes/workspace/olist_bronze/raw"

BRONZE_TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "sellers",
    "products",
    "category_translation"
]

CSV_TO_TABLE = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_sellers_dataset.csv": "sellers",
    "olist_products_dataset.csv": "products",
    "product_category_name_translation.csv": "category_translation",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate setup

# COMMAND ----------

# List schemas
display(spark.sql("SHOW SCHEMAS IN workspace"))

# COMMAND ----------

# list files in raw volume
files = dbutils.fs.ls(RAW_PATH)
for f in files:
    print(f"{f.name:50s} {f.size / 1024 / 1024:.2f} MB")