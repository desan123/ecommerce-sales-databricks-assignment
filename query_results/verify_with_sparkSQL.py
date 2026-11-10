# Databricks notebook source
# MAGIC %md
# MAGIC # Data Verification — SparkSQL

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/generic_functions

# COMMAND ----------


# COMMAND ----------

def show_query(title: str, sql: str):
    """Run a SparkSQL query and display the result with a header."""
    print(f"\n{'#' * 65}")
    print(f"  {title}")
    print(f"{'#' * 65}")
    df = spark.sql(sql)
    df.display()
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many jobs are currently open?

# COMMAND ----------
SILVER_DB = {{configs.config.silver_db}}
GOLD_DB = {{configs.config.gold_db}}
# COMMAND ----------


show_query(
    "Open Job Count",
    f"""
    SELECT COUNT(*) AS open_jobs
    FROM {SILVER_DB}.jobs
    WHERE LOWER(status) = 'open'
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 5 departments by number of applications

# COMMAND ----------

show_query(
    "Top 5 Departments by Applications",
    f"""
    SELECT
        j.department,
        COUNT(a.application_id) AS total_applications
    FROM {SILVER_DB}.applications a
    JOIN {SILVER_DB}.jobs j ON a.job_id = j.job_id
    WHERE j.department IS NOT NULL
    GROUP BY j.department
    ORDER BY total_applications DESC
    LIMIT 5
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Candidates who applied to more than 3 jobs

# COMMAND ----------

show_query(
    "Candidates Applied to More Than 3 Jobs",
    f"""
    SELECT
        c.candidate_id,
        c.first_name,
        c.last_name,
        COUNT(DISTINCT a.job_id) AS jobs_applied
    FROM {SILVER_DB}.candidates c
    JOIN {SILVER_DB}.applications a ON c.candidate_id = a.candidate_id
    GROUP BY c.candidate_id, c.first_name, c.last_name
    HAVING COUNT(DISTINCT a.job_id) > 3
    ORDER BY jobs_applied DESC
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Row Counts

# COMMAND ----------

show_query(
    "Gold Layer Row Counts",
    f"""
    SELECT 'dim_job' AS table_name, COUNT(*) AS row_count FROM {GOLD_DB}.dim_job
    UNION ALL
    SELECT 'dim_candidate' AS table_name, COUNT(*) AS row_count FROM {GOLD_DB}.dim_candidate
    UNION ALL
    SELECT 'fact_applications' AS table_name, COUNT(*) AS row_count FROM {GOLD_DB}.fact_applications
    UNION ALL
    SELECT 'fact_workflow_events' AS table_name, COUNT(*) AS row_count FROM {GOLD_DB}.fact_workflow_events
    ORDER BY table_name
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold dim_job: Sample

# COMMAND ----------

show_query(
    "Gold dim_job: Sample",
    f"SELECT * FROM {GOLD_DB}.dim_job"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold fact_applications: Hired vs Not Hired

# COMMAND ----------

show_query(
    "fact_applications: Hired vs Not Hired",
    f"""
    SELECT
        is_hired,
        COUNT(*) AS count
    FROM {GOLD_DB}.fact_applications
    GROUP BY is_hired
    ORDER BY is_hired
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold dim_candidate: Sample

# COMMAND ----------

show_query(
    "Gold dim_candidate: Sample",
    f"""
    SELECT candidate_id, first_name, last_name, email
    FROM {GOLD_DB}.dim_candidate
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-to-Hire by Department

# COMMAND ----------

show_query(
    "Time-to-Hire by Department",
    f"""
    SELECT
        j.department,
        COUNT(a.application_id) AS hired_count,
        ROUND(AVG(DATEDIFF(a.hired_date, a.apply_date)), 1) AS avg_days_to_hire,
        MIN(DATEDIFF(a.hired_date, a.apply_date)) AS min_days,
        MAX(DATEDIFF(a.hired_date, a.apply_date)) AS max_days
    FROM {GOLD_DB}.fact_applications a
    JOIN {GOLD_DB}.dim_job j ON a.job_id = j.job_id
    WHERE a.is_hired = TRUE
      AND a.hired_date IS NOT NULL
      AND a.apply_date IS NOT NULL
      AND DATEDIFF(a.hired_date, a.apply_date) >= 0
    GROUP BY j.department
    ORDER BY avg_days_to_hire
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-to-Hire by Job

# COMMAND ----------

show_query(
    "TTime-to-Hire by Job",
    f"""
    SELECT
        a.job_id,
        j.title,
        j.department,
        COUNT(a.application_id) AS hired_count,
        ROUND(AVG(DATEDIFF(a.hired_date, a.apply_date)), 1) AS avg_days_to_hire
    FROM {GOLD_DB}.fact_applications a
    JOIN {GOLD_DB}.dim_job j ON a.job_id = j.job_id
    WHERE a.is_hired = TRUE
      AND a.hired_date IS NOT NULL
      AND a.apply_date IS NOT NULL
      AND DATEDIFF(a.hired_date, a.apply_date) >= 0
    GROUP BY a.job_id, j.title, j.department
    ORDER BY avg_days_to_hire ASC
    LIMIT 10
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC Sample Anomalous Events (hired before applied)

# COMMAND ----------
show_query(
    "Sample Anomalous Events (hired before applied)",
    f"""
    SELECT
        event_id,
        application_id,
        old_status,
        new_status,
        event_timestamp,
        anomaly_reason
    FROM {SILVER_DB}.workflow_events
    WHERE anomaly_flag = TRUE
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC DQ Log: Latest Run Results

# COMMAND ----------

show_query(
    "DQ Check Results (latest run)",
    f"""
    SELECT
        check_name,
        passed,
        message,
        failed_count,
        run_timestamp
    FROM {SILVER_DB}.dq_check
    WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {SILVER_DB}.dq_check)
    ORDER BY passed ASC, check_name ASC
    """
)
