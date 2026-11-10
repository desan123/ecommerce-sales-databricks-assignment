CREATE TABLE IF NOT EXISTS gold_lhdev.agg_profit (
  year INT,
  category STRING,
  sub_category STRING,
  customer_id STRING,
  customer_name STRING,
  total_profit DECIMAL(10,2),
  total_revenue DECIMAL(10,2),
  total_cost DECIMAL(10,2),
  order_count BIGINT
)
USING DELTA
COMMENT 'Gold layer - profit aggregated by year, category, sub-category, and customer';
