CREATE TABLE IF NOT EXISTS ecom_dev.gold_lhdev.dim_customer_enriched (
  customer_id STRING NOT NULL,
  customer_name STRING,
  segment STRING,
  country STRING,
  city STRING,
  state STRING,
  region STRING,
  total_orders BIGINT,
  total_revenue DECIMAL(18,2),
  total_profit DECIMAL(18,2)
)
USING DELTA
COMMENT 'Gold layer - enriched customer dimension with aggregated metrics';
