CREATE TABLE IF NOT EXISTS ecom_dev.silver_lhdev.products (
  product_id STRING NOT NULL,
  category STRING,
  sub_category STRING,
  product_name STRING,
  state STRING,
  unit_price DECIMAL(10,2)
)
USING DELTA
COMMENT 'Silver layer - cleaned products data';
