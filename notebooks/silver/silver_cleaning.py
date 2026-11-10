# COMMAND ----------

# MAGIC %run /notebooks/util_lib/transformations

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/data_quality

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/generic_functions

# COMMAND ----------
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md ## Read Bronze

# COMMAND ----------

base_orders = spark.table("{{configs.config.bronze_db}}.orders")
base_customers = spark.table("{{configs.config.bronze_db}}.customers")
base_products = spark.table("{{configs.config.bronze_db}}.products")

# COMMAND ----------

# MAGIC %md ## Apply Transformations

# COMMAND ----------

s_orders = clean_orders(base_orders)
s_customers = clean_customers(base_customers)
s_products = clean_products(base_products)

# COMMAND ----------

# MAGIC %md ## Data Quality Checks

# COMMAND ----------

dq_results = []

# Uniqueness checks
dq_results.append(check_uniqueness(s_orders, ["order_id"], "silver_orders"))
dq_results.append(check_uniqueness(s_customers, ["customer_id"], "silver_customers"))
dq_results.append(check_uniqueness(s_products, ["product_id"], "silver_products"))

print(dq_results)

# Not-null PK checks
dq_results.append(check_not_null(s_orders, ["order_id", "customer_id", "product_id"], "silver_orders"))
dq_results.append(check_not_null(s_customers, ["customer_id"], "silver_customers"))
dq_results.append(check_not_null(s_products, ["product_id"], "silver_products"))

print(dq_results)

# Volume checks
dq_results.append(check_volume(s_orders, "silver_orders", min_rows=1, max_rows=10000000))
dq_results.append(check_volume(s_customers, "silver_customers", min_rows=1, max_rows=1000000))
dq_results.append(check_volume(s_products, "silver_products", min_rows=1, max_rows=100000))

print(dq_results)

# Referential integrity checks
dq_results.append(check_referential_integrity(s_orders, "customer_id", s_customers, "customer_id", "orders", "customers"))
dq_results.append(check_referential_integrity(s_orders, "product_id", s_products, "product_id", "orders", "products"))

# Log to Delta audit table
dq_df = run_dq_suite(spark, dq_results, suite_name="silver_cleaning")
(
    dq_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("{{configs.config.silver_db}}.dq_check")
)

# Fail fast if any critical check fails (uniqueness or not-null)
critical_failures = [r for r in dq_results if not r.passed and
                     ("uniqueness" in r.check_name or "not_null" in r.check_name)]
if critical_failures:
    raise RuntimeError(f"Critical DQ failures: {[r.check_name for r in critical_failures]}")

# COMMAND ----------

# MAGIC %md ## Write Silver

# COMMAND ----------

upsert_logic(s_orders, "{{configs.config.silver_db}}.orders", ["order_id"])
upsert_logic(s_customers, "{{configs.config.silver_db}}.customers", ["customer_id"])
upsert_logic(s_products, "{{configs.config.silver_db}}.products", ["product_id"])
