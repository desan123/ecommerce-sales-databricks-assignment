CREATE TABLE IF NOT EXISTS bronze_lhdev.orders (
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
COMMENT 'Bronze layer - raw orders data from source (keeps original column names with spaces)';
