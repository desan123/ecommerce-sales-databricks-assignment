CREATE TABLE IF NOT EXISTS bronze_lhdev.orders (
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  order_date STRING,
  quantity STRING,
  unit_price STRING,
  unit_cost STRING
)
USING DELTA
COMMENT 'Bronze layer - raw orders data from source systems';
