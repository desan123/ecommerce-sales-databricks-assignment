CREATE TABLE IF NOT EXISTS silver_lhdev.products (
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  sub_category STRING
)
USING DELTA
COMMENT 'Silver layer - cleaned products data';
