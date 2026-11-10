DROP TABLE IF EXISTS {{configs.config.bronze_db}}.education;
CREATE EXTERNAL TABLE {{configs.config.bronze_db}}.education (
    candidate_id STRING NOT NULL,
    degree       STRING,
    institution  STRING,
    year         STRING
)
USING DELTA
LOCATION '{{ configs.config.bronze_path }}/education'
COMMENT 'Bronze education schema'