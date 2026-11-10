DROP TABLE IF EXISTS {{configs.config.silver_db}}.applications;
CREATE EXTERNAL TABLE {{configs.config.silver_db}}.applications (
    application_id STRING NOT NULL,
    job_id         STRING NOT NULL,
    candidate_id   STRING NOT NULL,
    apply_date     DATE
)
USING DELTA
LOCATION '{{ configs.config.silver_path }}/applications'
COMMENT 'Silver applications schema'