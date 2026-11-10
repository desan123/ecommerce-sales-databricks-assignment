DROP TABLE IF EXISTS {{configs.config.silver_db}}.candidates;
CREATE EXTERNAL TABLE {{configs.config.silver_db}}.candidates (
    candidate_id STRING NOT NULL,
    first_name   STRING,
    last_name    STRING,
    email        STRING,
    phone        STRING,
    skills       STRING
)
USING DELTA
LOCATION '{{ configs.config.silver_path }}/candidates'
COMMENT 'Silver candicates schema'