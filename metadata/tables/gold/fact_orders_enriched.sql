CREATE TABLE IF NOT EXISTS gold_lhdev.fact_orders_enriched (
  order_id STRING NOT NULL,
  order_date DATE,
  year INT,
  customer_id STRING NOT NULL,
  customer_name STRING,
  country STRING,
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  sub_category STRING,
  quantity INT,
  unit_price DECIMAL(10,2),
  unit_cost DECIMAL(10,2),
  revenue DECIMAL(10,2),
  cost DECIMAL(10,2),
  profit DECIMAL(10,2)
)
USING DELTA
COMMENT 'Gold layer - enriched orders fact with profit and dimensional attributes';
