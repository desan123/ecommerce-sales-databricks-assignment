CREATE TABLE IF NOT EXISTS ecom_dev.bronze_lhdev.orders (
  `Row ID` STRING,
  `Order ID` STRING NOT NULL,
  `Order Date` STRING,
  `Ship Date` STRING,
  `Ship Mode` STRING,
  `Customer ID` STRING NOT NULL,
  `Product ID` STRING NOT NULL,
  `Quantity` STRING,
  `Price` STRING,
  `Discount` STRING,
  `Profit` STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
)
COMMENT 'Bronze layer - raw orders data with column mapping enabled for spaces in column names';
