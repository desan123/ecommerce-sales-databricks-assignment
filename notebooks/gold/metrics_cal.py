from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/utils

# COMMAND ----------

# MAGIC %md ## Time to Hire per job and dept calculation

# COMMAND ----------

fact_applications = spark.table("{{configs.config.gold_db}}.fact_applications")
dim_job = spark.table("{{configs.config.gold_db}}.dim_job")

by_job, by_department = compute_time_to_hire(fact_applications, dim_job)

# COMMAND ----------

# MAGIC %md ## Time to Hire by Job

# COMMAND ----------

by_job.orderBy(F.col("avg_days_to_hire").asc()).display()

# COMMAND ----------

# MAGIC %md ## Time to Hire by Department

# COMMAND ----------

by_department.orderBy(F.col("avg_days_to_hire").asc()).display()

# COMMAND ----------

# MAGIC %md ## Summary stats

# COMMAND ----------

overall = (
    fact_applications
    .filter(F.col("is_hired") == True)
    .withColumn("days_to_hire", F.datediff(F.col("hired_date"), F.col("apply_date")))
    .filter(F.col("days_to_hire") >= 0)
    .agg(
        F.count("application_id").alias("total_hires"),
        F.round(F.avg("days_to_hire"), 1).alias("avg_days_to_hire"),
        F.min("days_to_hire").alias("min_days_to_hire"),
        F.max("days_to_hire").alias("max_days_to_hire"),
        F.round(F.percentile_approx("days_to_hire", 0.5), 1).alias("median_days_to_hire"),
    )
)
overall.display()
