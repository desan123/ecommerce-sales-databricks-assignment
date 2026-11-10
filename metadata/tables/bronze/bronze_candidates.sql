DROP TABLE IF EXISTS {{configs.config.bronze_db}}.candidates;
CREATE EXTERNAL TABLE {{configs.config.bronze_db}}.candidates (
    candidate_id STRING NOT NULL,
    first_name   STRING,
    last_name    STRING,
    email        STRING,
    phone        STRING,
    skills       STRING 
)
USING DELTA
LOCATION '{{ configs.config.bronze_path }}/candidates'
COMMENT 'Bronze candicates schema'