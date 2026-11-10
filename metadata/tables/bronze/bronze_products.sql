CREATE TABLE IF NOT EXISTS bronze_lhdev.products (
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  sub_category STRING
)
USING DELTA
COMMENT 'Bronze layer - raw products data from source systems';
