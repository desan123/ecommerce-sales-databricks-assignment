CREATE TABLE IF NOT EXISTS silver_lhdev.orders (
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  order_date DATE,
  quantity INT,
  unit_price DECIMAL(10,2),
  unit_cost DECIMAL(10,2)
)
USING DELTA
COMMENT 'Silver layer - cleaned and typed orders data';
