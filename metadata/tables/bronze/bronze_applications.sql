DROP TABLE IF EXISTS {{configs.config.bronze_db}}.applications;
CREATE EXTERNAL TABLE {{configs.config.bronze_db}}.applications (
    application_id STRING NOT NULL,
    job_id         STRING NOT NULL,
    candidate_id   STRING NOT NULL,
    apply_date     STRING
)
USING DELTA
LOCATION '{{ configs.config.bronze_path }}/applications'
COMMENT 'Bronze applications schema'