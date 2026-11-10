# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC
# MAGIC This notebook loads raw data from CSV/JSON/Excel files into Bronze Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Paths

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("raw_path", "/Volumes/ecom_dev/default/files/", "Raw Data Path")
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

# MAGIC %run /Workspace/Users/sks12101997@gmail.com/assignment/notebooks/util_lib/schema_definitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Generic Functions

# COMMAND ----------

# MAGIC %run /Workspace/Users/sks12101997@gmail.com/assignment/notebooks/util_lib/generic_functions

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

print(f"Orders read: {orders_raw.count()}")
display(orders_raw.limit(5))

# COMMAND ----------

# Upsert to Bronze table
# NOTE: "Order ID" is NOT unique (9994 line items, 5009 orders); "Row ID" is the unique key
upsert_logic_bronze(orders_raw, f"{BRONZE_DB}.orders", ["Row ID"], BRONZE_ORDERS_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Customers (Excel/CSV Format)
# MAGIC
# MAGIC **Note**: Databricks Community Edition may not have spark-excel library.
# MAGIC - If you have Excel file: Convert to CSV first in Excel
# MAGIC - Upload Customer.csv instead of Customer.xlsx

# COMMAND ----------

# DBTITLE 1,Cell 12
# Try to read Excel, fallback to CSV if Excel reader not available

# Try Excel format first
customers_raw = (
    spark.read
    .option("headerRows", 1)
    .excel(f"{RAW_BASE}/Customer.xlsx")
)
print("Successfully read Customer.xlsx using Excel reader")

print(f"Customers read: {customers_raw.count()} records")
display(customers_raw.limit(5))
customers_raw.printSchema()

# COMMAND ----------

# Upsert to Bronze table
upsert_logic_bronze(
    customers_raw,
    f"{BRONZE_DB}.customers",
    ["Customer ID"],
    BRONZE_CUSTOMERS_SCHEMA,
)
print("Customers table created/updated successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Products (CSV Format)

# COMMAND ----------

# Read Products from CSV
products_raw = (
    spark.read
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE") 
    .schema(BRONZE_PRODUCTS_SCHEMA)
    .csv(f"{RAW_BASE}/Products.csv")
)

print(f"Products read: {products_raw.count()} records")
display(products_raw.limit(100))

# COMMAND ----------

# Upsert to Bronze table
upsert_logic_bronze(
    products_raw,
    f"{BRONZE_DB}.products",
    # "Product ID" alone is not unique (same id sold with different name/state/price)
    ["Product ID", "Product Name", "State"],
    BRONZE_PRODUCTS_SCHEMA,
)
print("Products table created/updated successfully")

# COMMAND ----------
