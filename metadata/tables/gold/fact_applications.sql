DROP TABLE IF EXISTS {{configs.config.gold_db}}.fact_applications;
CREATE EXTERNAL TABLE {{configs.config.gold_db}}.fact_applications(
    application_id STRING  NOT NULL,
    job_id         STRING  NOT NULL,
    candidate_id   STRING  NOT NULL,
    apply_date     DATE,
    hired_date     DATE,
    current_status STRING,
    is_hired       BOOLEAN NOT NULL
)
USING DELTA
LOCATION '{{ configs.config.gold_path }}/fact_applications'
TBLPROPERTIES (
    'lakehouse.primary.key' = 'application_id',
    'lakehouse.foreign.keys' = 'job_id:{{configs.config.gold_db}}.dim_job(job_id),candidate_id:{{configs.config.gold_db}}.dim_candidate(candidate_id)',
    'delta.enableChangeDataFeed' = 'true'
);