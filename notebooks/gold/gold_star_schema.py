# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Layer - Star Schema
# MAGIC
# MAGIC This notebook creates enriched dimensions and fact tables for analytics.
# MAGIC
# MAGIC **Created Tables:**
# MAGIC 1. dim_customer_enriched - Customers with order statistics
# MAGIC 2. dim_product_enriched - Products with sales statistics
# MAGIC 3. fact_orders_enriched - Orders with profit, customer & product details
# MAGIC 4. agg_profit - Profit aggregated by Year, Category, Sub-Category, Customer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Databases

# COMMAND ----------

dbutils.widgets.text("silver_db", "silver_lhdev", "Silver Database")
dbutils.widgets.text("gold_db", "gold_lhdev", "Gold Database")

SILVER_DB = dbutils.widgets.get("silver_db")
GOLD_DB = dbutils.widgets.get("gold_db")

print(f"Silver Database: {SILVER_DB}")
print(f"Gold Database: {GOLD_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Utilities

# COMMAND ----------

# MAGIC %run ../util_lib/transformations

# COMMAND ----------

# MAGIC %run ../util_lib/generic_functions

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Tables

# COMMAND ----------

print("Reading Silver tables...")

silver_orders = spark.table(f"{SILVER_DB}.orders")
silver_customers = spark.table(f"{SILVER_DB}.customers")
silver_products = spark.table(f"{SILVER_DB}.products")

print(f"✓ Orders:    {silver_orders.count():,} records")
print(f"✓ Customers: {silver_customers.count():,} records")
print(f"✓ Products:  {silver_products.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enriched Customer Dimension

# COMMAND ----------

print("\nBuilding enriched customer dimension...")

dim_customer_enriched = build_enriched_customer(silver_customers, silver_orders)

print(f"✓ Enriched customers: {dim_customer_enriched.count():,} records")
display(dim_customer_enriched.limit(10))

# COMMAND ----------

upsert_logic(dim_customer_enriched, f"{GOLD_DB}.dim_customer_enriched", ["customer_id"])
print(f"✓ Written to {GOLD_DB}.dim_customer_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Enriched Product Dimension

# COMMAND ----------

print("\nBuilding enriched product dimension...")

dim_product_enriched = build_enriched_product(silver_products, silver_orders)

print(f"✓ Enriched products: {dim_product_enriched.count():,} records")
display(dim_product_enriched.limit(10))

# COMMAND ----------

upsert_logic(dim_product_enriched, f"{GOLD_DB}.dim_product_enriched", ["product_id"])
print(f"✓ Written to {GOLD_DB}.dim_product_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enriched Orders Fact Table
# MAGIC
# MAGIC **Includes:**
# MAGIC - Order information
# MAGIC - **Profit rounded to 2 decimal places** ✓
# MAGIC - Customer details
# MAGIC - Product details

# COMMAND ----------

print("\nBuilding enriched orders fact table...")

fact_orders_enriched = build_enriched_orders(silver_orders, silver_customers, silver_products)

print(f"✓ Enriched orders: {fact_orders_enriched.count():,} records")
display(fact_orders_enriched.limit(10))

# COMMAND ----------

upsert_logic(fact_orders_enriched, f"{GOLD_DB}.fact_orders_enriched", ["order_id"])
print(f"✓ Written to {GOLD_DB}.fact_orders_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregate Profit Table

# COMMAND ----------

print("\nBuilding aggregate profit table...")

agg_profit = build_aggregate_profit(fact_orders_enriched)

print(f"✓ Aggregate profit: {agg_profit.count():,} records")
display(agg_profit.orderBy(F.desc("total_profit")).limit(20))

# COMMAND ----------

upsert_logic(agg_profit, f"{GOLD_DB}.agg_profit", ["year", "category", "sub_category", "customer_id"])
print(f"✓ Written to {GOLD_DB}.agg_profit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gold Layer Complete!
