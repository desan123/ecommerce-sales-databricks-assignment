CREATE TABLE IF NOT EXISTS ecom_dev.gold_lhdev.agg_profit (
  year INT,
  category STRING,
  sub_category STRING,
  customer_id STRING,
  customer_name STRING,
  total_profit DECIMAL(18,2),
  total_revenue DECIMAL(18,2),
  total_discount DECIMAL(18,2),
  order_count BIGINT
)
USING DELTA
COMMENT 'Gold layer - profit aggregated by year, category, sub-category and customer';
