# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - gold aggregations
# MAGIC creates denormalized fact table and aggregated business metrics

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, countDistinct, sum, avg, month, year, max,
    date_trunc, when, current_timestamp
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##fct_orders (fact_table)

# COMMAND ----------

df_orders = spark.table("workspace.olist_silver.orders")
df_customers = spark.table("workspace.olist_silver.customers")
df_reviews = spark.table("workspace.olist_silver.reviews")
df_payments = spark.table("workspace.olist_silver.payments")
df_items = spark.table("workspace.olist_silver.order_items")

# COMMAND ----------

# criando 2 df temporário para usar na tabela fct

# df temporário 
payment_agg = (
    df_payments
    .groupBy("order_id")
    .agg(
        sum("payment_value").alias("total_payment"),
        max("payment_installments").alias("num_installments_max"),
    )
)

# df temporário 
items_agg = (
    df_items
    .groupBy("order_id")
    .agg(
        count("*").alias("total_items"),
        sum("price").alias("total_products_value"),
        sum("freight_value").alias("total_freight_value")
    )
)

# COMMAND ----------

# tabela principal (fact)
fct_orders = (
    df_orders
    .join(df_customers, "customer_id", "left")
    .join(payment_agg, "order_id", "left")
    .join(df_reviews.select("order_id", "review_score"), "order_id", "left")
    .join(items_agg, "order_id", "left")
    .select(
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "delivery_days",
        "is_late",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "total_payment",
        "num_installments_max",
        "review_score",
        "total_items",
        "total_products_value",
        "total_freight_value"
    )
)

fct_orders.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_gold.fct_orders")
print(f"fct_orders: {spark.table('workspace.olist_gold.fct_orders').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## agg_monthly_metrics

# COMMAND ----------

df_fct = spark.table("workspace.olist_gold.fct_orders")

# agregado mensal de vendas
agg_monthly = (
    df_fct
    .withColumn("month", date_trunc("month", "order_purchase_timestamp"))
    .groupBy("month")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("total_payment").alias("total_revenue"),
        avg("total_payment").alias("avg_ticket"),
        avg("review_score").alias("avg_review_score"),
        avg("delivery_days").alias("avg_delivery_days"),
        sum(when(col("is_late") == 1, 1).otherwise(0)).alias("late_deliveries"), # soma de atrasos por mês
        countDistinct("customer_unique_id").alias("unique_customers"),
    )
    .orderBy("month")
)

agg_monthly.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_gold.agg_monthly_metrics")
print(f"agg_monthly_metrics: {spark.table('workspace.olist_gold.agg_monthly_metrics').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## agg_seller_performance

# COMMAND ----------

df_sellers = spark.table("workspace.olist_silver.sellers")
df_items = spark.table("workspace.olist_silver.order_items")
df_orders = spark.table("workspace.olist_silver.orders")
df_reviews = spark.table("workspace.olist_silver.reviews")

# agregado de vendedores
agg_seller = (
    df_items
    .join(df_sellers, "seller_id", "inner")
    .join(df_orders.select("order_id", "delivery_days", "is_late"), "order_id", "inner")
    .join(df_reviews.select("order_id", "review_score"), "order_id", "left")
    .groupBy("seller_id", "seller_city", "seller_state")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        avg("price").alias("avg_ticket"),
        avg("review_score").alias("avg_review_score"),
        avg("delivery_days").alias("avg_delivery_days"),
        avg(when(col("is_late") == 1, 1).otherwise(0)).alias("late_delivery_rate"), # proporção de atrasos
    )
)

agg_seller.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_gold.agg_seller_performance")
print(f"agg_seller_performance: {spark.table('workspace.olist_gold.agg_seller_performance').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## agg_category_performance

# COMMAND ----------

df_products = spark.table("workspace.olist_silver.products")
df_items = spark.table("workspace.olist_silver.order_items")
df_orders = spark.table("workspace.olist_silver.orders")
df_reviews = spark.table("workspace.olist_silver.reviews")

# agregado por categoria
agg_category = (
    df_items
    .join(df_products.select("product_id", "product_category_name_english"), "product_id", "inner")
    .join(df_orders.select("order_id", "delivery_days"), "order_id", "inner")
    .join(df_reviews.select("order_id", "review_score"), "order_id", "left")
    .groupBy(col("product_category_name_english").alias("category"))
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        avg("price").alias("avg_price"),
        avg("review_score").alias("avg_review_score"),
        avg("freight_value").alias("avg_freight"),
    )
    .orderBy(col("total_revenue").desc())
)

agg_category.write.format("delta").mode("overwrite").saveAsTable("workspace.olist_gold.agg_category_performance")
print(f"agg_category_performance: {spark.table('workspace.olist_gold.agg_category_performance').count():,} rows")