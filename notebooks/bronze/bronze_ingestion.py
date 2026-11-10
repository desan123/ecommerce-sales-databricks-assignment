from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/schema_definitions

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/generic_functions

# COMMAND ----------

# MAGIC %md ## 1. Orders (from JSON)

# COMMAND ----------
RAW_BASE = "{{configs.config.file_path}}"

# Read Orders from JSON file
orders_raw = (
    spark.read
    .option("multiline", "true")
    .json(f"{RAW_BASE}/Orders.json", schema=BRONZE_ORDERS_SCHEMA)
)

upsert_logic_bronze(orders_raw, "{{configs.config.bronze_db}}.orders", ["Order ID"], BRONZE_ORDERS_SCHEMA)

# COMMAND ----------

# MAGIC %md ## 2. Customers (from Excel)
# MAGIC
# MAGIC **Note**: Databricks needs the spark-excel library to read Excel files directly.
# MAGIC If not available, please convert Customer.xlsx to Customer.csv in Excel and upload it.
# MAGIC
# MAGIC **Option 1**: Using spark-excel (if installed)
# MAGIC ```
# MAGIC customers_raw = (
# MAGIC     spark.read
# MAGIC     .format("com.crealytics.spark.excel")
# MAGIC     .option("header", "true")
# MAGIC     .option("inferSchema", "false")
# MAGIC     .schema(BRONZE_CUSTOMERS_SCHEMA)
# MAGIC     .load(f"{RAW_BASE}/Customer.xlsx")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Option 2**: Using CSV (recommended for Databricks Community Edition)

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
    print("✗ Excel reader not available. Trying Customer.csv...")
    customers_raw = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .schema(BRONZE_CUSTOMERS_SCHEMA)
        .csv(f"{RAW_BASE}/Customer.csv")
    )
    print("✓ Successfully read Customer.csv")

upsert_logic_bronze(
    customers_raw,
    "{{configs.config.bronze_db}}.customers",
    ["Customer ID"],
    BRONZE_CUSTOMERS_SCHEMA,
)

# COMMAND ----------

# MAGIC %md ## 3. Products (from CSV)

# COMMAND ----------

products_raw = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .schema(BRONZE_PRODUCTS_SCHEMA)
    .csv(f"{RAW_BASE}/Products.csv")
)

upsert_logic_bronze(
    products_raw,
    "{{configs.config.bronze_db}}.products",
    ["Product ID"],
    BRONZE_PRODUCTS_SCHEMA,
)

# COMMAND ----------

# MAGIC %md ## Verify Bronze Tables

# COMMAND ----------

print("=== Bronze Layer Summary ===")
print(f"Orders: {spark.table('{{configs.config.bronze_db}}.orders').count()} records")
print(f"Customers: {spark.table('{{configs.config.bronze_db}}.customers').count()} records")
print(f"Products: {spark.table('{{configs.config.bronze_db}}.products').count()} records")

# COMMAND ----------

# Show sample records
print("\n=== Sample Orders ===")
spark.table("{{configs.config.bronze_db}}.orders").show(5, truncate=False)

print("\n=== Sample Customers ===")
spark.table("{{configs.config.bronze_db}}.customers").show(5, truncate=False)

print("\n=== Sample Products ===")
spark.table("{{configs.config.bronze_db}}.products").show(5, truncate=False)
