CREATE TABLE IF NOT EXISTS bronze_lhdev.customers (
  `Customer ID` STRING NOT NULL,
  `Customer Name` STRING,
  `Segment` STRING,
  `Country` STRING,
  `City` STRING,
  `State` STRING,
  `Postal Code` STRING,
  `Region` STRING
)
USING DELTA
COMMENT 'Bronze layer - raw customers data from source (keeps original column names with spaces)';
