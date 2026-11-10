# Databricks notebook source

# MAGIC %md
# MAGIC # Data Quality Functions
# MAGIC
# MAGIC This notebook contains data quality check functions for validation.

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/utils

# COMMAND ----------

# ---------------------------------------------------------------------------
# Bronze → Silver transformations
# ---------------------------------------------------------------------------

def clean_orders(df: DataFrame) -> DataFrame:
    """
    Clean orders data from Bronze layer:
    - Rename columns to snake_case (remove spaces)
    - Drop exact duplicates on Order ID
    - Parse order_date and ship_date to DateType
    - Cast quantity to integer, price/discount/profit to decimal
    - Filter out invalid records (null PKs, negative quantities)

    Note: Profit is already calculated in the source data
    """
    # Rename columns to snake_case
    df = (df
        .withColumnRenamed("Row ID", "row_id")
        .withColumnRenamed("Order ID", "order_id")
        .withColumnRenamed("Order Date", "order_date")
        .withColumnRenamed("Ship Date", "ship_date")
        .withColumnRenamed("Ship Mode", "ship_mode")
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Product ID", "product_id")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("Price", "price")
        .withColumnRenamed("Discount", "discount")
        .withColumnRenamed("Profit", "profit")
    )

    # Drop duplicates
    df = df.dropDuplicates(["order_id"])

    # Type casting and parsing
    df = (df
        .withColumn("row_id", F.col("row_id").cast("integer"))
        .withColumn("order_date", parse_date_col("order_date"))
        .withColumn("ship_date", parse_date_col("ship_date"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("price", F.col("price").cast("decimal(10,2)"))
        .withColumn("discount", F.col("discount").cast("decimal(5,2)"))
        .withColumn("profit", F.round(F.col("profit").cast("decimal(10,2)"), 2))
    )

    # Filter out invalid records
    df = df.filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull() &
        (F.col("quantity") > 0)
    )

    return df


def clean_customers(df: DataFrame) -> DataFrame:
    """
    Clean customers data:
    - Rename columns to snake_case
    - Drop exact duplicates on Customer ID
    - NFC-normalise customer_name, country, city
    - Standardize segment to title-case
    - Filter out records with null customer_id
    """
    norm = get_normalize_udf()

    # Rename columns
    df = (df
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Customer Name", "customer_name")
        .withColumnRenamed("Segment", "segment")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Postal Code", "postal_code")
        .withColumnRenamed("Region", "region")
    )

    # Drop duplicates
    df = df.dropDuplicates(["customer_id"])

    # Normalize and standardize
    df = (df
        .withColumn("customer_name", norm(F.trim(F.col("customer_name"))))
        .withColumn("country", norm(F.trim(F.col("country"))))
        .withColumn("city", norm(F.trim(F.col("city"))))
        .withColumn("state", F.trim(F.col("state")))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("segment", F.initcap(F.trim(F.col("segment"))))
        .withColumn("postal_code", F.trim(F.col("postal_code")))
    )

    # Clean phone numbers if column exists
    if "Phone" in df.columns:
        df = df.withColumn("phone", clean_phone_col("Phone"))

    # Filter out records with null customer_id
    df = df.filter(F.col("customer_id").isNotNull())

    return df


def clean_products(df: DataFrame) -> DataFrame:
    """
    Clean products data:
    - Rename columns to snake_case
    - Drop exact duplicates on Product ID
    - NFC-normalise product_name, category, sub_category
    - Standardise category and sub_category to title-case
    - Cast Price per product to decimal as unit_price
    """
    norm = get_normalize_udf()

    # Rename columns
    df = (df
        .withColumnRenamed("Product ID", "product_id")
        .withColumnRenamed("Category", "category")
        .withColumnRenamed("Sub-Category", "sub_category")
        .withColumnRenamed("Product Name", "product_name")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Price per product", "unit_price")
    )

    # Drop duplicates
    df = df.dropDuplicates(["product_id"])

    # Normalize and standardize
    df = (df
        .withColumn("product_name", norm(F.trim(F.col("product_name"))))
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .withColumn("sub_category", F.initcap(F.trim(F.col("sub_category"))))
        .withColumn("state", F.trim(F.col("state")))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    )

    # Filter out records with null product_id
    df = df.filter(F.col("product_id").isNotNull())

    return df


# ---------------------------------------------------------------------------
# Gold layer transformations
# ---------------------------------------------------------------------------

def build_enriched_customer(silver_customers: DataFrame, silver_orders: DataFrame) -> DataFrame:
    """
    Builds the enriched customer dimension by aggregating order data.

    Calculates:
    - total_orders: count of orders per customer
    - total_revenue: sum of price (total price per order)
    - total_profit: sum of profit per customer
    """
    order_stats = (
        silver_orders
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("price").alias("total_revenue"),
            F.sum("profit").alias("total_profit")
        )
    )

    enriched = (
        silver_customers
        .join(order_stats, on="customer_id", how="left")
        .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0)))
        .withColumn("total_revenue", F.round(F.coalesce(F.col("total_revenue"), F.lit(0.0)), 2))
        .withColumn("total_profit", F.round(F.coalesce(F.col("total_profit"), F.lit(0.0)), 2))
    )

    return enriched.select(
        "customer_id", "customer_name", "segment", "country", "city", "state", "region",
        "total_orders", "total_revenue", "total_profit"
    )


def build_enriched_product(silver_products: DataFrame, silver_orders: DataFrame) -> DataFrame:
    """
    Builds the enriched product dimension by aggregating order data.

    Calculates:
    - total_quantity_sold: sum of quantity sold
    - total_revenue: sum of price (total revenue)
    - total_profit: sum of profit per product
    """
    product_stats = (
        silver_orders
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_quantity_sold"),
            F.sum("price").alias("total_revenue"),
            F.sum("profit").alias("total_profit")
        )
    )

    enriched = (
        silver_products
        .join(product_stats, on="product_id", how="left")
        .withColumn("total_quantity_sold", F.coalesce(F.col("total_quantity_sold"), F.lit(0)))
        .withColumn("total_revenue", F.round(F.coalesce(F.col("total_revenue"), F.lit(0.0)), 2))
        .withColumn("total_profit", F.round(F.coalesce(F.col("total_profit"), F.lit(0.0)), 2))
    )

    return enriched.select(
        "product_id", "product_name", "category", "sub_category",
        "total_quantity_sold", "total_revenue", "total_profit"
    )


def build_enriched_orders(
    silver_orders: DataFrame,
    silver_customers: DataFrame,
    silver_products: DataFrame
) -> DataFrame:
    """
    Builds the enriched fact table with:
    - Order information (order_id, order_date, year, month)
    - Profit rounded to 2 decimal places (already in source)
    - Customer details (name, country, region, segment)
    - Product details (name, category, sub_category)

    Note: Profit is already calculated in the source data and rounded to 2 decimals
    """
    enriched = (
        silver_orders
        # Join customer details
        .join(
            silver_customers.select("customer_id", "customer_name", "country", "region", "segment"),
            on="customer_id",
            how="left"
        )
        # Join product details
        .join(
            silver_products.select("product_id", "product_name", "category", "sub_category"),
            on="product_id",
            how="left"
        )
        # Extract year and month
        .withColumn("year", F.year(F.col("order_date")))
        .withColumn("month", F.month(F.col("order_date")))
        # Ensure profit is rounded to 2 decimals
        .withColumn("profit", F.round(F.col("profit"), 2))
    )

    return enriched.select(
        "order_id", "order_date", "year", "month",
        "customer_id", "customer_name", "country", "region", "segment",
        "product_id", "product_name", "category", "sub_category",
        "quantity", "price", "discount", "profit"
    )


def build_aggregate_profit(fact_orders_enriched: DataFrame) -> DataFrame:
    """
    Builds the aggregate table showing profit by:
    - Year
    - Product Category
    - Product Sub Category
    - Customer

    Aggregates:
    - total_profit: sum of profit
    - total_revenue: sum of price
    - total_discount: sum of discount
    - order_count: count of orders

    All decimals rounded to 2 places
    """
    aggregate = (
        fact_orders_enriched
        .groupBy("year", "category", "sub_category", "customer_id", "customer_name")
        .agg(
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.round(F.sum("price"), 2).alias("total_revenue"),
            F.round(F.sum("discount"), 2).alias("total_discount"),
            F.count("order_id").alias("order_count")
        )
    )

    return aggregate
