# Databricks notebook source

# MAGIC %md
# MAGIC # data quality check
# MAGIC validate silveer tables after transformation

# COMMAND ----------

# MAGIC %run ../notebooks/00_setup

# COMMAND ----------


from pyspark.sql.functions import col, min, max

# COMMAND ----------

results = []

def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    results.append({"check": name, "status": status, "detail": detail})
    print(f"[{status}] {name} {detail}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## primary key uniqueness

# COMMAND ----------

for table, pk in [
    ("orders", "order_id"),
    ("customers", "customer_id"),
    ("products", "product_id"),
    ("sellers", "seller_id"),
    ("reviews", "order_id"),
]:
    df = spark.table(f"workspace.olist_silver.{table}")
    total = df.count()
    distinct = df.select(pk).distinct().count()
    check(f"{table}.{pk} unique", total == distinct, f"total={total:,} distinct={distinct:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## not null checks

# COMMAND ----------

not_null_checks = [
    ("orders", ["order_id", "customer_id", "order_status"]),
    ("customers", ["customer_id", "customer_unique_id"]),
    ("products", ["product_id"]),
    ("order_items", ["order_id", "product_id", "seller_id"]),
    ("payments", ["order_id", "payment_type"]),
    ("reviews", ["order_id", "review_score"]),
    ("sellers", ["seller_id"]),
]

for table, columns in not_null_checks:
    df = spark.table(f"workspace.olist_silver.{table}")
    for column in columns:
        nulls = df.filter(col(column).isNull()).count()
        check(f"{table}.{column} not null", nulls == 0, f"nulls={nulls}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## range checks

# COMMAND ----------



df_reviews = spark.table("workspace.olist_silver.reviews")
min_score = df_reviews.select(min("review_score")).first()[0]
max_score = df_reviews.select(max("review_score")).first()[0]
check("reviews.review_score range [1,5]", min_score >= 1 and max_score <=5, f"min={min_score} max={max_score}")

df_payments = spark.table("workspace.olist_silver.payments")
neg_payments = df_payments.filter(col("payment_value") < 0).count()
check("payments.payment_value >= 0", neg_payments == 0, f"negatives={neg_payments}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## referential integrity

# COMMAND ----------

df_items = spark.table("workspace.olist_silver.order_items")
df_orders = spark.table("workspace.olist_silver.orders")

orphan_items = df_items.join(df_orders, "order_id", "left_anti").count()
check("order_items.order_id exists in orders", orphan_items == 0, f"orphans={orphan_items}")

df_reviews = spark.table("workspace.olist_silver.reviews")
orphan_reviews = df_reviews.join(df_orders, "order_id", "left_anti").count()
check("reviews.order_id exists in orders", orphan_reviews == 0, f"orphans={orphan_reviews}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## consistency checks

# COMMAND ----------

df_orders = spark.table("workspace.olist_silver.orders")
delivered_no_date = (
    df_orders
    .filter((col("order_status") == "delivered") & col("delivery_days").isNull())
    .count()
)
check("delivered orders have delivery_days", delivered_no_date == 0, f"missing={delivered_no_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total = len(results)
passed = sum(1 for r in results if r["status"] == "PASS")
failed = sum(1 for r in results if r["status"] == "FAIL")
print(f"\n{'='*50}")
print(f"Total: {total} | Passed: {passed} | Failed: {failed}")
print(f"{'='*50}")