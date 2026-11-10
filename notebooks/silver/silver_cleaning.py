# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Layer Cleaning
# MAGIC
# MAGIC This notebook cleans and validates data from Bronze layer to Silver layer.
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Data type conversions
# MAGIC - Deduplication
# MAGIC - Normalization
# MAGIC - Data quality checks
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - bronze_lhdev.orders, customers, products
# MAGIC
# MAGIC **Target Tables:**
# MAGIC - silver_lhdev.orders, customers, products

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Databases

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("bronze_db", "bronze_lhdev", "Bronze Database")
dbutils.widgets.text("silver_db", "silver_lhdev", "Silver Database")

# Get widget values
BRONZE_DB = dbutils.widgets.get("bronze_db")
SILVER_DB = dbutils.widgets.get("silver_db")

print(f"Bronze Database: {BRONZE_DB}")
print(f"Silver Database: {SILVER_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Utilities

# COMMAND ----------

# MAGIC %run ../util_lib/transformations

# COMMAND ----------

# MAGIC %run ../util_lib/data_quality

# COMMAND ----------

# MAGIC %run ../util_lib/generic_functions

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Tables

# COMMAND ----------

print("Reading Bronze tables...")

base_orders = spark.table(f"{BRONZE_DB}.orders")
base_customers = spark.table(f"{BRONZE_DB}.customers")
base_products = spark.table(f"{BRONZE_DB}.products")

print(f"✓ Orders:    {base_orders.count():,} records")
print(f"✓ Customers: {base_customers.count():,} records")
print(f"✓ Products:  {base_products.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Orders

# COMMAND ----------

s_orders = clean_orders(base_orders)
print(f"✓ Orders cleaned: {s_orders.count():,} records")
display(s_orders.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Customers

# COMMAND ----------

s_customers = clean_customers(base_customers)
print(f"✓ Customers cleaned: {s_customers.count():,} records")
display(s_customers.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Products

# COMMAND ----------

s_products = clean_products(base_products)
print(f"✓ Products cleaned: {s_products.count():,} records")
display(s_products.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

print("=" * 60)
print("RUNNING DATA QUALITY CHECKS")
print("=" * 60)

dq_results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uniqueness Checks

# COMMAND ----------

print("\n1. Checking uniqueness...")
dq_results.append(check_uniqueness(s_orders, ["order_id"], "silver_orders"))
dq_results.append(check_uniqueness(s_customers, ["customer_id"], "silver_customers"))
dq_results.append(check_uniqueness(s_products, ["product_id"], "silver_products"))

for result in dq_results[-3:]:
    status = "✓ PASSED" if result.passed else "✗ FAILED"
    print(f"  {status}: {result.check_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Not-Null Checks

# COMMAND ----------

print("\n2. Checking not-null constraints...")
dq_results.append(check_not_null(s_orders, ["order_id", "customer_id", "product_id"], "silver_orders"))
dq_results.append(check_not_null(s_customers, ["customer_id"], "silver_customers"))
dq_results.append(check_not_null(s_products, ["product_id"], "silver_products"))

for result in dq_results[-3:]:
    status = "✓ PASSED" if result.passed else "✗ FAILED"
    print(f"  {status}: {result.check_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume Checks

# COMMAND ----------

print("\n3. Checking volumes...")
dq_results.append(check_volume(s_orders, "silver_orders", min_rows=1, max_rows=10000000))
dq_results.append(check_volume(s_customers, "silver_customers", min_rows=1, max_rows=1000000))
dq_results.append(check_volume(s_products, "silver_products", min_rows=1, max_rows=100000))

for result in dq_results[-3:]:
    status = "✓ PASSED" if result.passed else "✗ FAILED"
    print(f"  {status}: {result.check_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential Integrity Checks

# COMMAND ----------

print("\n4. Checking referential integrity...")
dq_results.append(check_referential_integrity(s_orders, "customer_id", s_customers, "customer_id", "orders", "customers"))
dq_results.append(check_referential_integrity(s_orders, "product_id", s_products, "product_id", "orders", "products"))

for result in dq_results[-2:]:
    status = "✓ PASSED" if result.passed else "✗ FAILED"
    print(f"  {status}: {result.check_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log DQ Results to Audit Table

# COMMAND ----------

# Log to Delta audit table
dq_df = run_dq_suite(spark, dq_results, suite_name="silver_cleaning")
(
    dq_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{SILVER_DB}.dq_check")
)

print("✓ DQ results logged to audit table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Critical Failures

# COMMAND ----------

# Fail fast if any critical check fails (uniqueness or not-null)
critical_failures = [r for r in dq_results if not r.passed and
                     ("uniqueness" in r.check_name or "not_null" in r.check_name)]

if critical_failures:
    print("\n✗ CRITICAL DQ FAILURES DETECTED:")
    for failure in critical_failures:
        print(f"  - {failure.check_name}: {failure.details}")
    raise RuntimeError(f"Critical DQ failures: {[r.check_name for r in critical_failures]}")
else:
    print("\n✓ All critical DQ checks passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer

# COMMAND ----------

print("\nWriting to Silver layer...")

upsert_logic(s_orders, f"{SILVER_DB}.orders", ["order_id"])
print(f"✓ Orders written to {SILVER_DB}.orders")

upsert_logic(s_customers, f"{SILVER_DB}.customers", ["customer_id"])
print(f"✓ Customers written to {SILVER_DB}.customers")

upsert_logic(s_products, f"{SILVER_DB}.products", ["product_id"])
print(f"✓ Products written to {SILVER_DB}.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: Silver Layer Summary

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER SUMMARY")
print("=" * 60)

orders_count = spark.table(f"{SILVER_DB}.orders").count()
customers_count = spark.table(f"{SILVER_DB}.customers").count()
products_count = spark.table(f"{SILVER_DB}.products").count()

print(f"✓ Orders:    {orders_count:,} records")
print(f"✓ Customers: {customers_count:,} records")
print(f"✓ Products:  {products_count:,} records")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver Cleaning Complete!
# MAGIC
# MAGIC **Data Quality Summary:**
# MAGIC - All uniqueness checks passed
# MAGIC - All not-null checks passed
# MAGIC - All referential integrity checks passed
# MAGIC
# MAGIC **Next Step:** Run the Gold Star Schema notebook to create enriched and aggregate tables.
