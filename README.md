# E-commerce Sales Data Processing with Databricks

**Author:** Santosh Kumar Shaw  
**Date:** Updated for E-commerce Assignment

---

## Overview

This project implements a scalable, efficient, and reliable data engineering solution using Databricks to process and analyze e-commerce sales data. The solution provides valuable insights for business stakeholders through a medallion architecture (Bronze → Silver → Gold).

The pipeline processes orders, customers, and products data to create enriched dimensions, fact tables with profit calculations, and aggregated analytics tables.

---

## Architecture

```
Raw Files (CSV)
        │
        ▼
  Bronze Layer  ──── raw ingestion, schema enforcement
        │
        ▼
  Silver Layer  ──── cleaned, validated, typed data
        │
        ▼
  Gold Layer    ──── Enriched dimensions + facts
                     + Aggregate profit tables
                     + SQL analytics queries
```

**Storage Format: Delta Lake**

Delta Lake provides:
- **ACID transactions** for reliable upsert operations
- **Schema evolution** for handling data changes
- **Time-travel** for auditing and rollback
- **Performance optimizations** through Z-ordering and compaction

---

## Project Structure

```
assignment/
├── configs/
│   └── config.yaml                          # Configuration for database paths
├── dataset/
│   ├── customers.csv                        # Sample customer data (15 records)
│   ├── orders.csv                          # Sample orders (50 transactions, 2023-2024)
│   └── products.csv                        # Sample product catalog (20 products)
├── metadata/
│   └── tables/
│       ├── bronze/                         # Bronze layer DDL scripts
│       │   ├── bronze_orders.sql
│       │   ├── bronze_customers.sql
│       │   └── bronze_products.sql
│       ├── silver/                         # Silver layer DDL scripts
│       │   ├── silver_orders.sql
│       │   ├── silver_customers.sql
│       │   └── silver_products.sql
│       ├── gold/                           # Gold layer DDL scripts
│       │   ├── dim_customer_enriched.sql
│       │   ├── dim_product_enriched.sql
│       │   ├── fact_orders_enriched.sql
│       │   └── agg_profit.sql
│       └── dq_results/
│           └── dq_check.sql                # Data quality audit table
├── notebooks/
│   ├── bronze/
│   │   └── bronze_ingestion.py             # Load raw CSV → Bronze Delta
│   ├── silver/
│   │   └── silver_cleaning.py              # Clean, validate, type cast
│   ├── gold/
│   │   ├── gold_star_schema.py             # Create enriched + aggregate tables
│   │   └── sql_profit_queries.py           # SQL analytics queries
│   ├── tests/
│   │   └── test_transformations.py         # Comprehensive PySpark unit tests
│   └── util_lib/
│       ├── schema_definitions.py           # All StructType schemas
│       ├── transformations.py              # Business transformation logic
│       ├── data_quality.py                 # DQ check functions
│       ├── generic_functions.py            # Reusable utilities (upsert logic)
│       └── utils.py                        # Date parsing, normalization
└── README.md                                # This file
```

---

## Data Model

### Bronze Layer (Raw Data)
All string types, raw from CSV:
- **orders**: order_id, customer_id, product_id, order_date, quantity, unit_price, unit_cost
- **customers**: customer_id, customer_name, country, city, email
- **products**: product_id, product_name, category, sub_category

### Silver Layer (Cleaned Data)
Typed, validated, deduplicated:
- **orders**: order_date → DateType, quantity → IntegerType, prices → DecimalType(10,2)
- **customers**: normalized names, lowercased emails
- **products**: title-cased categories

### Gold Layer (Business Analytics)

#### Enriched Dimensions
1. **dim_customer_enriched**
   - Customer base attributes + total_orders, total_revenue

2. **dim_product_enriched**
   - Product base attributes + total_quantity_sold, total_revenue

#### Enriched Fact Table
3. **fact_orders_enriched**
   - Order information (order_id, order_date, year)
   - Customer details (customer_name, country)
   - Product details (product_name, category, sub_category)
   - Financial metrics:
     - revenue = quantity × unit_price
     - cost = quantity × unit_cost
     - **profit = revenue - cost (rounded to 2 decimals)**

#### Aggregate Table
4. **agg_profit**
   - Dimensions: year, category, sub_category, customer_id, customer_name
   - Metrics: total_profit, total_revenue, total_cost, order_count
   - All aggregates rounded to 2 decimal places

---

## Assignment Requirements Implementation

### Task 1: Create raw tables for each source dataset
Implemented in `notebooks/bronze/bronze_ingestion.py`:
- `bronze_lhdev.orders`
- `bronze_lhdev.customers`
- `bronze_lhdev.products`

### Task 2: Create enriched tables for customers and products
Implemented in `notebooks/gold/gold_star_schema.py`:
- `gold_lhdev.dim_customer_enriched` (with total_orders, total_revenue)
- `gold_lhdev.dim_product_enriched` (with total_quantity_sold, total_revenue)

### Task 3: Create enriched table with order info, profit, customer & product details
Implemented as `gold_lhdev.fact_orders_enriched`:
- Order information
- **Profit rounded to 2 decimal places**
- Customer name and country
- Product category and sub category

### Task 4: Create aggregate table showing profit by Year, Category, Sub Category, Customer
Implemented as `gold_lhdev.agg_profit`:
- Aggregated by: year, category, sub_category, customer
- Metrics: total_profit, total_revenue, total_cost, order_count

### Task 5: SQL queries for profit aggregations
Implemented in `notebooks/gold/sql_profit_queries.py`:
1. Profit by Year
2. Profit by Year + Product Category
3. Profit by Customer
4. Profit by Customer + Year

### Task 6: Unit Tests (PySpark)
Implemented in `notebooks/tests/test_transformations.py`:
- Test classes for all transformations
- Covers happy path, edge cases, data validation
- Uses PySpark (not SQL) for all tests

---

## How to Run

### Prerequisites
- Databricks workspace with cluster
- PySpark environment
- Delta Lake support

### Step 1: Configuration
Update `configs/config.yaml`:
```yaml
environment: dev
bronze_db: bronze_lhdev
silver_db: silver_lhdev
gold_db: gold_lhdev
file_path: dbfs:/FileStore/ecommerce/raw     # Your path
bronze_path: dbfs:/delta/bronze
silver_path: dbfs:/delta/silver
gold_path: dbfs:/delta/gold
```

### Step 2: Upload Data
Upload datasets to your configured `file_path`:
```
dbfs:/FileStore/ecommerce/raw/
├── customers.csv
├── orders.csv
└── products.csv
```

### Step 3: Execute Notebooks in Order

1. **Bronze Ingestion**
   ```python
   %run /notebooks/bronze/bronze_ingestion.py
   ```
   - Loads raw CSV files
   - Creates bronze Delta tables
   - Uses upsert logic (idempotent)

2. **Silver Cleaning**
   ```python
   %run /notebooks/silver/silver_cleaning.py
   ```
   - Applies transformations
   - Runs data quality checks
   - Writes to silver layer

3. **Gold Star Schema**
   ```python
   %run /notebooks/gold/gold_star_schema.py
   ```
   - Creates enriched dimensions
   - Creates enriched fact table
   - Creates aggregate profit table

4. **SQL Profit Queries**
   ```python
   %run /notebooks/gold/sql_profit_queries.py
   ```
   - Runs SQL analytics queries
   - Saves results for reporting

5. **Run Unit Tests (Optional)**
   ```python
   %run /notebooks/tests/test_transformations.py
   ```
   - Validates all transformations
   - Ensures correctness

---

## Data Quality Checks

Implemented in `notebooks/util_lib/data_quality.py`:

### Uniqueness Checks
- Primary key uniqueness (order_id, customer_id, product_id)

### Not-Null Checks
- Critical fields validation

### Volume Checks
- Min/max row count validation

### Referential Integrity
- Foreign key validation (orders → customers, orders → products)

### Data Validation
- Negative quantity/price filtering
- Invalid date handling
- Duplicate removal

All DQ results logged to `silver_lhdev.dq_check` table.

---

## Key Transformations

### Bronze → Silver
```python
def clean_orders(df):
    # 1. Type casting: string → date, int, decimal
    # 2. Deduplication by order_id
    # 3. Filter invalid records (null PKs, negative values)
    # 4. Return cleaned DataFrame
```

### Silver → Gold
```python
def build_enriched_orders(orders, customers, products):
    # 1. Join orders with customers (get name, country)
    # 2. Join with products (get category, sub_category)
    # 3. Calculate: revenue, cost, profit (rounded to 2 decimals)
    # 4. Extract year from order_date
    # 5. Return enriched fact table
```

```python
def build_aggregate_profit(fact_orders):
    # Aggregate by: year, category, sub_category, customer
    # Sum: profit, revenue, cost
    # Count: orders
    # Round all decimals to 2 places
```

---

## Sample SQL Queries

### Profit by Year
```sql
SELECT year, ROUND(SUM(total_profit), 2) AS total_profit
FROM gold_lhdev.agg_profit
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year DESC;
```

### Top 10 Profitable Customers
```sql
SELECT customer_name, ROUND(SUM(total_profit), 2) AS total_profit
FROM gold_lhdev.agg_profit
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC
LIMIT 10;
```

### Profit by Product Category
```sql
SELECT category, sub_category, ROUND(SUM(total_profit), 2) AS total_profit
FROM gold_lhdev.agg_profit
GROUP BY category, sub_category
ORDER BY total_profit DESC;
```

### Customer + Year Profit Trend
```sql
SELECT customer_name, year, ROUND(SUM(total_profit), 2) AS total_profit
FROM gold_lhdev.agg_profit
WHERE customer_id = 'C001'
GROUP BY customer_id, customer_name, year
ORDER BY year;
```

---

## Test Coverage

Comprehensive PySpark unit tests in `notebooks/tests/test_transformations.py`:

### Test Classes
1. **TestCleanOrders** (6 test cases)
   - Basic cleaning
   - Duplicate removal
   - Invalid record filtering
   - Date parsing
   - Type casting validation

2. **TestCleanCustomers** (4 test cases)
   - Basic cleaning
   - Duplicate removal
   - Null ID filtering
   - Email lowercasing

3. **TestCleanProducts** (4 test cases)
   - Basic cleaning
   - Duplicate removal
   - Null ID filtering
   - Title-case categories

4. **TestBuildEnrichedCustomer** (2 test cases)
   - Basic enrichment with orders
   - Customer with no orders (edge case)

5. **TestBuildEnrichedOrders** (3 test cases)
   - Basic enrichment
   - Profit calculation validation
   - Year extraction

6. **TestBuildAggregateProfit** (1 test case)
   - Aggregation by all dimensions

### Running Tests
```python
%run /notebooks/tests/test_transformations.py
```

Output shows:
- Test results (pass/fail)
- Detailed error messages
- Assertion validations

---

## Performance Optimizations

1. **Delta Format**: ACID transactions, efficient upserts
2. **Upsert Logic**: Merge-based incremental updates
3. **Partitioning**: Optional by date for large datasets
4. **Schema Evolution**: Handles new columns gracefully
5. **Predicate Pushdown**: Filter at source
6. **Caching**: Reuse Silver tables in Gold layer

---

## Business Insights Enabled

The solution answers:
- **Which products are most profitable?**
- **Which customers generate the highest profit?**
- **How does profitability trend over time (by year)?**
- **Which product categories drive the most revenue?**
- **What is the profit margin by sub-category?**
- **Which customers have declining/growing profit trends?**

---

## Author
Santosh Kumar Shaw

---

## AI Statement

AI tools were used to assist with:
- Refining documentation
- Suggesting test case scenarios

All core logic, transformations, and business rules were implemented and validated manually.
