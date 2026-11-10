DROP TABLE IF EXISTS {{configs.config.gold_db}}.dim_candidate;
CREATE EXTERNAL TABLE {{configs.config.gold_db}}.dim_candidate (
    candidate_id STRING NOT NULL,
    first_name   STRING,
    last_name    STRING,
    email        STRING,
    phone        STRING,
    skills       STRING,
    education    STRING
)
USING DELTA
CLUSTER BY (candidate_id)
LOCATION '{{ configs.config.gold_path }}/dim_candidate'
TBLPROPERTIES (
    'lakehouse.primary.key' = 'candidate_id',
    'delta.enableChangeDataFeed' = 'true'
)