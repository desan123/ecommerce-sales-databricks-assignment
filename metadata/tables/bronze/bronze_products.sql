CREATE TABLE IF NOT EXISTS ecom_dev.bronze_lhdev.products (
  `Product ID` STRING NOT NULL,
  `Category` STRING,
  `Sub-Category` STRING,
  `Product Name` STRING,
  `State` STRING,
  `Price per product` STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
)
COMMENT 'Bronze layer - raw products data with column mapping enabled';
