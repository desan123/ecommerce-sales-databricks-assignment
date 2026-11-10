# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC
# MAGIC This notebook loads raw data from CSV/JSON/Excel files into Bronze Delta tables.
# MAGIC
# MAGIC **Source Files:**
# MAGIC - Orders.json
# MAGIC - Products.csv
# MAGIC - Customer.csv (or Customer.xlsx)
# MAGIC
# MAGIC **Target Tables:**
# MAGIC - bronze_lhdev.orders
# MAGIC - bronze_lhdev.customers
# MAGIC - bronze_lhdev.products

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Paths

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("raw_path", "dbfs:/FileStore/tables/", "Raw Data Path")
dbutils.widgets.text("bronze_db", "bronze_lhdev", "Bronze Database")

# Get widget values
RAW_BASE = dbutils.widgets.get("raw_path")
BRONZE_DB = dbutils.widgets.get("bronze_db")

print(f"Raw Data Path: {RAW_BASE}")
print(f"Bronze Database: {BRONZE_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Schema Definitions

# COMMAND ----------

# MAGIC %run ../util_lib/schema_definitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Generic Functions

# COMMAND ----------

# MAGIC %run ../util_lib/generic_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Orders (JSON Format)

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Read Orders from JSON file
orders_raw = (
    spark.read
    .option("multiline", "true")
    .json(f"{RAW_BASE}/Orders.json", schema=BRONZE_ORDERS_SCHEMA)
)

print(f"Orders read: {orders_raw.count()} records")
display(orders_raw.limit(5))

# COMMAND ----------

# Upsert to Bronze table
upsert_logic_bronze(orders_raw, f"{BRONZE_DB}.orders", ["Order ID"], BRONZE_ORDERS_SCHEMA)
print("✓ Orders table created/updated successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Customers (Excel/CSV Format)
# MAGIC
# MAGIC **Note**: Databricks Community Edition may not have spark-excel library.
# MAGIC - If you have Excel file: Convert to CSV first in Excel
# MAGIC - Upload Customer.csv instead of Customer.xlsx

# COMMAND ----------

# Try to read Excel, fallback to CSV if Excel reader not available
try:
    # Try Excel format first
    customers_raw = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(BRONZE_CUSTOMERS_SCHEMA)
        .load(f"{RAW_BASE}/Customer.xlsx")
    )
    print("✓ Successfully read Customer.xlsx using Excel reader")
except:
    # Fallback to CSV
    print("Excel reader not available. Reading Customer.csv...")
    customers_raw = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .schema(BRONZE_CUSTOMERS_SCHEMA)
        .csv(f"{RAW_BASE}/Customer.csv")
    )
    print("✓ Successfully read Customer.csv")

print(f"Customers read: {customers_raw.count()} records")
display(customers_raw.limit(5))

# COMMAND ----------

# Upsert to Bronze table
upsert_logic_bronze(
    customers_raw,
    f"{BRONZE_DB}.customers",
    ["Customer ID"],
    BRONZE_CUSTOMERS_SCHEMA,
)
print("✓ Customers table created/updated successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Products (CSV Format)

# COMMAND ----------

# Read Products from CSV
products_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .schema(BRONZE_PRODUCTS_SCHEMA)
    .csv(f"{RAW_BASE}/Products.csv")
)

print(f"Products read: {products_raw.count()} records")
display(products_raw.limit(5))

# COMMAND ----------

# Upsert to Bronze table
upsert_logic_bronze(
    products_raw,
    f"{BRONZE_DB}.products",
    ["Product ID"],
    BRONZE_PRODUCTS_SCHEMA,
)
print("✓ Products table created/updated successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification: Bronze Layer Summary

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER SUMMARY")
print("=" * 60)

orders_count = spark.table(f"{BRONZE_DB}.orders").count()
customers_count = spark.table(f"{BRONZE_DB}.customers").count()
products_count = spark.table(f"{BRONZE_DB}.products").count()

print(f"✓ Orders:    {orders_count:,} records")
print(f"✓ Customers: {customers_count:,} records")
print(f"✓ Products:  {products_count:,} records")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Orders

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.orders").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Customers

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.customers").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Products

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.products").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Bronze Ingestion Complete!
# MAGIC
# MAGIC **Next Step:** Run the Silver Cleaning notebook to clean and validate data.
