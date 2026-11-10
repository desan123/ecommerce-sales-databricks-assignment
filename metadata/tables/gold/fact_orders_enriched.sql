CREATE TABLE IF NOT EXISTS ecom_dev.gold_lhdev.fact_orders_enriched (
  row_id INT,
  order_id STRING NOT NULL,
  order_date DATE,
  year INT,
  month INT,
  customer_id STRING NOT NULL,
  customer_name STRING,
  country STRING,
  region STRING,
  segment STRING,
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  sub_category STRING,
  quantity INT,
  price DECIMAL(10,2),
  discount DECIMAL(5,2),
  profit DECIMAL(10,2)
)
USING DELTA
COMMENT 'Gold layer - enriched orders fact (one row per order line, key = row_id)';
