# Databricks notebook source

# MAGIC %md
# MAGIC # Schema Definitions
# MAGIC
# MAGIC This notebook contains all schema definitions for Bronze, Silver, and Gold layers.

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Schemas (Raw Data - All Strings)

# COMMAND ----------

BRONZE_ORDERS_SCHEMA = StructType([
    StructField("Row ID",       StringType(), nullable=True),
    StructField("Order ID",     StringType(), nullable=False),
    StructField("Order Date",   StringType(), nullable=True),
    StructField("Ship Date",    StringType(), nullable=True),
    StructField("Ship Mode",    StringType(), nullable=True),
    StructField("Customer ID",  StringType(), nullable=False),
    StructField("Product ID",   StringType(), nullable=False),
    StructField("Quantity",     StringType(), nullable=True),
    StructField("Price",        StringType(), nullable=True),
    StructField("Discount",     StringType(), nullable=True),
    StructField("Profit",       StringType(), nullable=True),
])

# COMMAND ----------

BRONZE_CUSTOMERS_SCHEMA = StructType([
    StructField("Customer ID",   StringType(), nullable=False),
    StructField("Customer Name", StringType(), nullable=True),
    StructField("email",         StringType(), nullable=True),
    StructField("phone",         StringType(), nullable=True),
    StructField("address",       StringType(), nullable=True),
    StructField("Segment",       StringType(), nullable=True),
    StructField("Country",       StringType(), nullable=True),
    StructField("City",          StringType(), nullable=True),
    StructField("State",         StringType(), nullable=True),
    StructField("Postal Code",   StringType(), nullable=True),
    StructField("Region",        StringType(), nullable=True),
])

# COMMAND ----------

BRONZE_PRODUCTS_SCHEMA = StructType([
    StructField("Product ID",         StringType(), nullable=False),
    StructField("Category",           StringType(), nullable=True),
    StructField("Sub-Category",       StringType(), nullable=True),
    StructField("Product Name",       StringType(), nullable=True),
    StructField("State",              StringType(), nullable=True),
    StructField("Price per product",  StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Schemas (Typed, Cleaned Data)

# COMMAND ----------

SILVER_ORDERS_SCHEMA = StructType([
    StructField("row_id",       IntegerType(), nullable=True),
    StructField("order_id",     StringType(), nullable=False),
    StructField("order_date",   DateType(),   nullable=True),
    StructField("ship_date",    DateType(),   nullable=True),
    StructField("ship_mode",    StringType(), nullable=True),
    StructField("customer_id",  StringType(), nullable=False),
    StructField("product_id",   StringType(), nullable=False),
    StructField("quantity",     IntegerType(), nullable=True),
    StructField("price",        DecimalType(10, 2), nullable=True),
    StructField("discount",     DecimalType(5, 2), nullable=True),
    StructField("profit",       DecimalType(10, 2), nullable=True),
])

# COMMAND ----------

SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",   StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=True),
    StructField("email",         StringType(), nullable=True),
    StructField("phone",         StringType(), nullable=True),
    StructField("address",       StringType(), nullable=True),
    StructField("segment",       StringType(), nullable=True),
    StructField("country",       StringType(), nullable=True),
    StructField("city",          StringType(), nullable=True),
    StructField("state",         StringType(), nullable=True),
    StructField("postal_code",   StringType(), nullable=True),
    StructField("region",        StringType(), nullable=True),
])

# COMMAND ----------

SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id",     StringType(), nullable=False),
    StructField("category",       StringType(), nullable=True),
    StructField("sub_category",   StringType(), nullable=True),
    StructField("product_name",   StringType(), nullable=True),
    StructField("state",          StringType(), nullable=True),
    StructField("unit_price",     DecimalType(10, 2), nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Schemas (Analytics-Ready)

# COMMAND ----------

GOLD_DIM_CUSTOMER_ENRICHED_SCHEMA = StructType([
    StructField("customer_id",    StringType(), nullable=False),
    StructField("customer_name",  StringType(), nullable=True),
    StructField("segment",        StringType(), nullable=True),
    StructField("country",        StringType(), nullable=True),
    StructField("city",           StringType(), nullable=True),
    StructField("state",          StringType(), nullable=True),
    StructField("region",         StringType(), nullable=True),
    StructField("total_orders",   LongType(),   nullable=True),
    StructField("total_revenue",  DecimalType(10, 2), nullable=True),
    StructField("total_profit",   DecimalType(10, 2), nullable=True),
])

# COMMAND ----------

GOLD_DIM_PRODUCT_ENRICHED_SCHEMA = StructType([
    StructField("product_id",          StringType(), nullable=False),
    StructField("product_name",        StringType(), nullable=True),
    StructField("category",            StringType(), nullable=True),
    StructField("sub_category",        StringType(), nullable=True),
    StructField("total_quantity_sold", LongType(), nullable=True),
    StructField("total_revenue",       DecimalType(10, 2), nullable=True),
    StructField("total_profit",        DecimalType(10, 2), nullable=True),
])

# COMMAND ----------

GOLD_FACT_ORDERS_ENRICHED_SCHEMA = StructType([
    StructField("row_id",         IntegerType(), nullable=True),
    StructField("order_id",       StringType(), nullable=False),
    StructField("order_date",     DateType(),   nullable=True),
    StructField("year",           IntegerType(), nullable=True),
    StructField("month",          IntegerType(), nullable=True),
    StructField("customer_id",    StringType(), nullable=False),
    StructField("customer_name",  StringType(), nullable=True),
    StructField("country",        StringType(), nullable=True),
    StructField("region",         StringType(), nullable=True),
    StructField("segment",        StringType(), nullable=True),
    StructField("product_id",     StringType(), nullable=False),
    StructField("product_name",   StringType(), nullable=True),
    StructField("category",       StringType(), nullable=True),
    StructField("sub_category",   StringType(), nullable=True),
    StructField("quantity",       IntegerType(), nullable=True),
    StructField("price",          DecimalType(10, 2), nullable=True),
    StructField("discount",       DecimalType(5, 2), nullable=True),
    StructField("profit",         DecimalType(10, 2), nullable=True),
])

# COMMAND ----------

GOLD_AGG_PROFIT_SCHEMA = StructType([
    StructField("year",           IntegerType(), nullable=True),
    StructField("category",       StringType(), nullable=True),
    StructField("sub_category",   StringType(), nullable=True),
    StructField("customer_id",    StringType(), nullable=True),
    StructField("customer_name",  StringType(), nullable=True),
    StructField("total_profit",   DecimalType(10, 2), nullable=True),
    StructField("total_revenue",  DecimalType(10, 2), nullable=True),
    StructField("total_discount", DecimalType(10, 2), nullable=True),
    StructField("order_count",    LongType(),   nullable=True),
])
