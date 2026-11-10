CREATE TABLE IF NOT EXISTS ecom_dev.silver_lhdev.orders (
  row_id INT,
  order_id STRING NOT NULL,
  order_date DATE,
  ship_date DATE,
  ship_mode STRING,
  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  quantity INT,
  price DECIMAL(10,2),
  discount DECIMAL(5,2),
  profit DECIMAL(10,2)
)
USING DELTA
COMMENT 'Silver layer - cleaned and typed orders data (one row per order line, key = row_id)';
