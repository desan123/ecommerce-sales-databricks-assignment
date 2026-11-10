# Databricks notebook source

# MAGIC %md
# MAGIC # SQL Profit Analysis Queries
# MAGIC
# MAGIC Profit aggregates written in SQL (via `spark.sql`) on top of `agg_profit`.
# MAGIC
# MAGIC `agg_profit` is grain: year x category x sub_category x customer, so summing
# MAGIC `total_profit` to any coarser grain is exact. `order_count` is intentionally
# MAGIC not re-summed here (a distinct count cannot be added across groups).

# COMMAND ----------

dbutils.widgets.text("gold_db", "ecom_dev.gold_lhdev", "Gold Database")
GOLD_DB = dbutils.widgets.get("gold_db")
print(f"Gold Database: {GOLD_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 1: Profit by Year

# COMMAND ----------

profit_by_year = spark.sql(f"""
SELECT
    year,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM {GOLD_DB}.agg_profit
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year
""")
display(profit_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 2: Profit by Year + Product Category

# COMMAND ----------

profit_by_year_category = spark.sql(f"""
SELECT
    year,
    category,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM {GOLD_DB}.agg_profit
WHERE year IS NOT NULL AND category IS NOT NULL
GROUP BY year, category
ORDER BY year, category
""")
display(profit_by_year_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3: Profit by Customer

# COMMAND ----------

profit_by_customer = spark.sql(f"""
SELECT
    customer_id,
    customer_name,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM {GOLD_DB}.agg_profit
WHERE customer_id IS NOT NULL
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC
""")
display(profit_by_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 4: Profit by Customer + Year

# COMMAND ----------

profit_by_customer_year = spark.sql(f"""
SELECT
    customer_id,
    customer_name,
    year,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM {GOLD_DB}.agg_profit
WHERE customer_id IS NOT NULL AND year IS NOT NULL
GROUP BY customer_id, customer_name, year
ORDER BY customer_id, year
""")
display(profit_by_customer_year)
