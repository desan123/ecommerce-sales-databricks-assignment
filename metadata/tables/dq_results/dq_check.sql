DROP TABLE IF EXISTS {{configs.config.silver_db}}.dq_check;
CREATE EXTERNAL TABLE {{configs.config.silver_db}}.dq_check (
    suite_name STRING NOT NULL,
    check_name STRING,
    passed BOOLEAN,
    message STRING,
    failed_count INT,
    run_timestamp STRING
)
USING DELTA
LOCATION '{{ configs.config.silver_path }}/dq_check'
COMMENT 'DQ checks table'