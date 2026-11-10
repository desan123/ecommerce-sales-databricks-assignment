DROP TABLE IF EXISTS {{configs.config.bronze_db}}.jobs;
CREATE EXTERNAL TABLE {{configs.config.bronze_db}}.jobs (
    job_id       STRING  NOT NULL,
    title        STRING,
    department   STRING,
    posted_date  STRING,
    status       STRING
)
USING DELTA
LOCATION '{{ configs.config.bronze_path }}/jobs'
COMMENT 'Bronze jobs schema'