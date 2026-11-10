DROP TABLE IF EXISTS {{configs.config.silver_db}}.education;
CREATE EXTERNAL TABLE {{configs.config.silver_db}}.education (
    candidate_id STRING  NOT NULL,
    degree       STRING,
    institution  STRING,
    year         INT
)
USING DELTA
LOCATION '{{ configs.config.silver_path }}/education'
COMMENT 'Silver education schema'