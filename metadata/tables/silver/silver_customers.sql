CREATE TABLE IF NOT EXISTS ecom_dev.silver_lhdev.customers (
  customer_id STRING NOT NULL,
  customer_name STRING,
  email STRING,
  phone STRING,
  address STRING,
  segment STRING,
  country STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  region STRING
)
USING DELTA
COMMENT 'Silver layer - cleaned customers data';
