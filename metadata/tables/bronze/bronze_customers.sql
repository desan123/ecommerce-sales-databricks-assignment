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
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
)
COMMENT 'Bronze layer - raw customers data with column mapping enabled';
