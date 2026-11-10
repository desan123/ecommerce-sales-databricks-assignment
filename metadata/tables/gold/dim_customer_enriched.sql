CREATE TABLE IF NOT EXISTS gold_lhdev.dim_customer_enriched (
  customer_id STRING NOT NULL,
  customer_name STRING,
  country STRING,
  city STRING,
  email STRING,
  total_orders BIGINT,
  total_revenue DECIMAL(10,2)
)
USING DELTA
COMMENT 'Gold layer - enriched customer dimension with aggregated metrics';
