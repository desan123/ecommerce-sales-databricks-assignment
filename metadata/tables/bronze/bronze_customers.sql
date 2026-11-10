CREATE TABLE IF NOT EXISTS bronze_lhdev.customers (
  customer_id STRING NOT NULL,
  customer_name STRING,
  country STRING,
  city STRING,
  email STRING
)
USING DELTA
COMMENT 'Bronze layer - raw customers data from source systems';
