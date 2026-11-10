CREATE TABLE IF NOT EXISTS silver_lhdev.customers (
  customer_id STRING NOT NULL,
  customer_name STRING,
  country STRING,
  city STRING,
  email STRING
)
USING DELTA
COMMENT 'Silver layer - cleaned customers data';
