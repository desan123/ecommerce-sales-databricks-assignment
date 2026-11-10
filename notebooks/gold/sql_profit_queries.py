# Databricks notebook source

# MAGIC %md
# MAGIC # SQL Profit Analysis Queries
# MAGIC
# MAGIC This notebook contains SQL queries for profit analysis as per assignment requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("gold_db", "gold_lhdev", "Gold Database")
GOLD_DB = dbutils.widgets.get("gold_db")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 1: Profit by Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     year,
# MAGIC     ROUND(SUM(total_profit), 2) AS total_profit,
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue,
# MAGIC     SUM(order_count) AS order_count
# MAGIC FROM gold_lhdev.agg_profit
# MAGIC WHERE year IS NOT NULL
# MAGIC GROUP BY year
# MAGIC ORDER BY year DESC

# COMMAND ----------

profit_by_year = spark.sql(f"""
SELECT
    year,
    ROUND(SUM(total_profit), 2) AS total_profit,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    SUM(order_count) AS order_count
FROM {GOLD_DB}.agg_profit
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year DESC
""")

profit_by_year.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.profit_by_year")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 2: Profit by Year + Product Category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     year,
# MAGIC     category,
# MAGIC     ROUND(SUM(total_profit), 2) AS total_profit,
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue,
# MAGIC     SUM(order_count) AS order_count
# MAGIC FROM gold_lhdev.agg_profit
# MAGIC WHERE year IS NOT NULL AND category IS NOT NULL
# MAGIC GROUP BY year, category
# MAGIC ORDER BY year DESC, total_profit DESC

# COMMAND ----------

profit_by_year_category = spark.sql(f"""
SELECT
    year,
    category,
    ROUND(SUM(total_profit), 2) AS total_profit,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    SUM(order_count) AS order_count
FROM {GOLD_DB}.agg_profit
WHERE year IS NOT NULL AND category IS NOT NULL
GROUP BY year, category
ORDER BY year DESC, total_profit DESC
""")

profit_by_year_category.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.profit_by_year_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3: Profit by Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     ROUND(SUM(total_profit), 2) AS total_profit,
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue,
# MAGIC     SUM(order_count) AS order_count
# MAGIC FROM gold_lhdev.agg_profit
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC GROUP BY customer_id, customer_name
# MAGIC ORDER BY total_profit DESC
# MAGIC LIMIT 100

# COMMAND ----------

profit_by_customer = spark.sql(f"""
SELECT
    customer_id,
    customer_name,
    ROUND(SUM(total_profit), 2) AS total_profit,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    SUM(order_count) AS order_count
FROM {GOLD_DB}.agg_profit
WHERE customer_id IS NOT NULL
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC
""")

profit_by_customer.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.profit_by_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 4: Profit by Customer + Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     year,
# MAGIC     ROUND(SUM(total_profit), 2) AS total_profit,
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue,
# MAGIC     SUM(order_count) AS order_count
# MAGIC FROM gold_lhdev.agg_profit
# MAGIC WHERE customer_id IS NOT NULL AND year IS NOT NULL
# MAGIC GROUP BY customer_id, customer_name, year
# MAGIC ORDER BY total_profit DESC, year DESC
# MAGIC LIMIT 100

# COMMAND ----------

profit_by_customer_year = spark.sql(f"""
SELECT
    customer_id,
    customer_name,
    year,
    ROUND(SUM(total_profit), 2) AS total_profit,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    SUM(order_count) AS order_count
FROM {GOLD_DB}.agg_profit
WHERE customer_id IS NOT NULL AND year IS NOT NULL
GROUP BY customer_id, customer_name, year
ORDER BY total_profit DESC, year DESC
""")

profit_by_customer_year.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.profit_by_customer_year")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ All SQL Queries Complete!
