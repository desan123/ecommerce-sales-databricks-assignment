CREATE TABLE IF NOT EXISTS bronze_lhdev.products (
  `Product ID` STRING NOT NULL,
  `Category` STRING,
  `Sub-Category` STRING,
  `Product Name` STRING,
  `State` STRING,
  `Price per product` STRING
)
USING DELTA
COMMENT 'Bronze layer - raw products data from source (keeps original column names with spaces)';
