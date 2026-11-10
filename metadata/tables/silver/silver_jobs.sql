DROP TABLE IF EXISTS {{configs.config.silver_db}}.jobs;
CREATE EXTERNAL TABLE {{configs.config.silver_db}}.jobs (
    job_id       STRING,
    title        STRING,
    department   STRING,
    posted_date  DATE,
    status       STRING
)
USING DELTA
LOCATION '{{ configs.config.silver_path }}/jobs'
COMMENT 'Silver jobs schema'