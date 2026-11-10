from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/transformations

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/generic_functions

# COMMAND ----------

# MAGIC %md ## Read Silver

# COMMAND ----------

silver_orders = spark.table("{{configs.config.silver_db}}.orders")
silver_customers = spark.table("{{configs.config.silver_db}}.customers")
silver_products = spark.table("{{configs.config.silver_db}}.products")

# COMMAND ----------

# MAGIC %md ## Enriched Customer Dimension

# COMMAND ----------

dim_customer_enriched = build_enriched_customer(silver_customers, silver_orders)
upsert_logic(dim_customer_enriched, "{{configs.config.gold_db}}.dim_customer_enriched", ["customer_id"])

# COMMAND ----------

# MAGIC %md ## Enriched Product Dimension

# COMMAND ----------

dim_product_enriched = build_enriched_product(silver_products, silver_orders)
upsert_logic(dim_product_enriched, "{{configs.config.gold_db}}.dim_product_enriched", ["product_id"])

# COMMAND ----------

# MAGIC %md ## Enriched Orders Fact Table

# COMMAND ----------

fact_orders_enriched = build_enriched_orders(silver_orders, silver_customers, silver_products)
upsert_logic(fact_orders_enriched, "{{configs.config.gold_db}}.fact_orders_enriched", ["order_id"])

# COMMAND ----------

# MAGIC %md ## Aggregate Profit Table

# COMMAND ----------

agg_profit = build_aggregate_profit(fact_orders_enriched)
upsert_logic(
    agg_profit,
    "{{configs.config.gold_db}}.agg_profit",
    ["year", "category", "sub_category", "customer_id"]
)
