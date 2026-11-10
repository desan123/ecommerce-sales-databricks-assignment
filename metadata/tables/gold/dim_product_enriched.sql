CREATE TABLE IF NOT EXISTS ecom_dev.gold_lhdev.dim_product_enriched (
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  sub_category STRING,
  total_quantity_sold BIGINT,
  total_revenue DECIMAL(18,2),
  total_profit DECIMAL(18,2)
)
USING DELTA
COMMENT 'Gold layer - enriched product dimension with aggregated metrics';
