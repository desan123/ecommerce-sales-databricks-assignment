"""
Quick script to inspect Customer.xlsx structure
Run this in a Databricks notebook to see the Customer columns
"""

# For Databricks
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read Customer Excel file (if you have the spark-excel library)
# If not, convert to CSV first in Excel and upload

print("=== Customer.xlsx inspection ===")
print("Please convert Customer.xlsx to Customer.csv in Excel and upload it")
print("OR install spark-excel library in Databricks")

# For now, let's assume standard customer fields based on e-commerce datasets
# Typical structure:
# Customer ID, Customer Name, Segment, Country, City, State, Postal Code, Region

print("\n=== Expected Customer columns ===")
expected_customer_cols = [
    "Customer ID",
    "Customer Name",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region"
]
for col in expected_customer_cols:
    print(f"  - {col}")
