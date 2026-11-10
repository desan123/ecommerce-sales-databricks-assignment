DROP TABLE IF EXISTS {{configs.config.gold_db}}.dim_job;
CREATE EXTERNAL TABLE {{configs.config.gold_db}}.dim_job (
    job_id      STRING NOT NULL,
    title       STRING,
    department  STRING,
    posted_date DATE,
    status      STRING
)
USING DELTA
LOCATION '{{ configs.config.gold_path }}/dim_job'
TBLPROPERTIES (
    'lakehouse.primary.key' = 'job_id',
    'delta.enableChangeDataFeed' = 'true'
);