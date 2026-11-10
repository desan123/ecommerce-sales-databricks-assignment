# E-commerce Sales Data Processing with Databricks

**Author:** Santosh Kumar Shaw

---

## Overview

A Databricks / PySpark pipeline that processes e-commerce orders, customers and products into analytics-ready Delta tables using a medallion architecture (Bronze → Silver → Gold).

```
Raw files (Orders.json, Customer.xlsx, Products.csv)
        │
        ▼
  Bronze  ── raw ingestion, all strings, original column names
        │
        ▼
  Silver  ── cleaned, typed, de-duplicated, DQ-checked
        │
        ▼
  Gold    ── enriched dimensions, enriched fact, profit aggregate,
             SQL profit outputs
```

All tables are Delta tables in the `ecom_dev` catalog (`bronze_lhdev`, `silver_lhdev`, `gold_lhdev`).

---

## Source Data

| File | Format | Rows | Notes |
|---|---|---|---|
| `dataset/Orders.json` | JSON array | 9,994 | One row per **order line**. `Order ID` is **not** unique (5,009 distinct); `Row ID` is. Dates are `d/M/yyyy`. |
| `dataset/Customer.xlsx` | Excel | 793 | Columns: Customer ID, Customer Name, email, phone, address, Segment, Country, City, State, Postal Code, Region. `Customer ID` is unique. Postal Code is numeric in Excel. |
| `dataset/Products.csv` | CSV | 1,851 | `Product ID` is **not** unique (1,818 distinct): the same ID appears with different name, state and price. |

---

## Project Structure

```
assignment/
├── configs/config.yaml
├── dataset/                       # Orders.json, Customer.xlsx, Products.csv
├── metadata/tables/               # DDL scripts
│   ├── bronze/                    # orders, customers, products
│   ├── silver/                    # orders, customers, products
│   ├── gold/                      # dim_customer_enriched, dim_product_enriched,
│   │                              # fact_orders_enriched, agg_profit
│   └── dq_results/dq_check.sql
├── notebooks/
│   ├── bronze/bronze_ingestion.py
│   ├── silver/silver_cleaning.py
│   ├── gold/gold_star_schema.py
│   ├── gold/sql_profit_queries.py
│   ├── tests/test_transformations.py   # plain-Python unit tests (notebook)
│   └── util_lib/                  # schema_definitions, transformations, 
└── README.md
```

---

## Data Model

### Bronze (raw, all strings, original column names)
- `orders`: Row ID, Order ID, Order Date, Ship Date, Ship Mode, Customer ID, Product ID, Quantity, Price, Discount, Profit. Merge key: **Row ID**.
- `customers`: Customer ID, Customer Name, email, phone, address, Segment, Country, City, State, Postal Code, Region. Merge key: Customer ID.
- `products`: Product ID, Category, Sub-Category, Product Name, State, Price per product. Merge key: **Product ID + Product Name + State**.

Delta column mapping is enabled because column names contain spaces.

### Silver (snake_case, typed, cleaned)
- `orders`: `row_id` INT, `order_date`/`ship_date` DATE, `quantity` INT, `price` DECIMAL(10,2), `discount` DECIMAL(5,2), `profit` DECIMAL(10,2). Key: **row_id**.
- `customers`: trimmed and NFC-normalised names, lowercased `email`, `phone` reduced to 10 digits (else NULL), flattened `address`, title-cased `segment`.
- `products`: title-cased category / sub-category, `unit_price` DECIMAL(10,2). One row per `product_id`.

### Gold
| Table | Description |
|---|---|
| `dim_customer_enriched` | Customer attributes + `total_orders`, `total_revenue`, `total_profit` |
| `dim_product_enriched` | Product attributes + `total_quantity_sold`, `total_revenue`, `total_profit` |
| `fact_orders_enriched` | One row per order line: order info (`row_id`, `order_id`, `order_date`, `year`, `month`), **profit rounded to 2 decimals**, customer name / country / region / segment, product name / category / sub-category, quantity, price, discount |
| `agg_profit` | Grain: year × category × sub-category × customer. Metrics: `total_profit`, `total_revenue`, `total_discount`, `order_count` (distinct orders) |
| `profit_by_year`, `profit_by_year_category`, `profit_by_customer`, `profit_by_customer_year` | SQL outputs built on `agg_profit` |

Gold tables are fully rebuilt from Silver on each run (`overwrite_logic`), which keeps them idempotent and avoids MERGE issues with NULL keys.

---

## Assignment Requirements → Implementation

| Requirement | Where |
|---|---|
| Raw tables for each source dataset | `notebooks/bronze/bronze_ingestion.py` → `orders`, `customers`, `products` |
| Enriched customer and product tables | `gold_star_schema.py` → `dim_customer_enriched`, `dim_product_enriched` |
| Enriched orders table (order info, profit to 2 dp, customer name and country, product category and sub-category) | `gold_star_schema.py` → `fact_orders_enriched` |
| Aggregate by year, category, sub-category, customer | `gold_star_schema.py` → `agg_profit` |
| SQL: profit by Year; Year + Category; Customer; Customer + Year | `sql_profit_queries.py` (`spark.sql`) |
| PySpark (not SQL) for transformations | `util_lib/transformations.py` |
| Unit tests | `notebooks/tests/test_transformations.py` |

---

## How to Run

Set the widgets (defaults shown) and run the notebooks in this order. Notebook imports use absolute workspace paths (`/Workspace/Users/<user>/assignment/notebooks/...`); update them if your workspace path differs.

1. **Create DDLs** (optional): run the scripts under `metadata/tables`.
2. **`bronze/bronze_ingestion.py`** — widgets `raw_path` (default `/Volumes/ecom_dev/default/files/`), `bronze_db`. Reads Customer.xlsx with the Databricks Excel reader.
3. **`silver/silver_cleaning.py`** — widgets `bronze_db` / `silver_db` (default `ecom_dev.bronze_lhdev` / `ecom_dev.silver_lhdev`). Runs cleaning, DQ checks, logs to `dq_check`, writes Silver.
4. **`gold/gold_star_schema.py`** — widgets `silver_db` / `gold_db`.
5. **`gold/sql_profit_queries.py`** — widget `gold_db`. Ends with a reconciliation query: all four outputs and the fact table must show the same total profit.
6. **`tests/test_transformations.py`** — plain-Python tests (each prints `Test Passed` / `Test Failed`). The last cell prints a summary and fails the notebook if any test failed.

If you previously ran an older version, drop the existing bronze / silver / gold tables first (the schemas and merge keys changed).

---

## Data Quality & Assumptions

Checks (`util_lib/data_quality.py`), logged to `silver_lhdev.dq_check`:
- Uniqueness: orders on `row_id`, customers on `customer_id`, products on `product_id`
- Not-null on key columns
- Volume bounds
- Referential integrity: orders → customers and orders → products

Uniqueness and not-null failures stop the run. Referential integrity is logged only.

Assumptions and decisions:
- **Order ID repeats** across line items, so `Row ID` identifies a line. Order counts use `countDistinct(order_id)`.
- **Product ID repeats** with different state / name / price, and orders carry no state. Silver keeps one deterministic row per `product_id` (lowest state, then name). Bronze keeps all variants.
- **Price** is treated as the line total, so revenue = sum of `Price`.
- **Profit** is supplied in the source; it is cast to DECIMAL(10,2) and rounded to 2 decimals, not recomputed.
- **Missing references:** about 204 order lines reference product IDs that are not in Products.csv. They are kept and labelled `Unknown` in the fact table so profit totals reconcile with the source.
- **Dates** are parsed against several formats; `d/M/yyyy` has priority. Unparseable dates become NULL.
- Invalid rows (null keys, quantity ≤ 0) are filtered out in Silver.

---

## Unit Tests

`notebooks/tests/test_transformations.py` (plain Python classes with try/except/else, PySpark, run on a Databricks cluster via `%run` of `util_lib`):

- **TestNormalizeUnicode (5):** None, plain ASCII, NFC normalisation, Chinese characters, empty string
- **TestParseDateCol (4):** d/M/yyyy, d/M priority over M/d, ISO format, invalid / None → NULL
- **TestCleanPhoneCol (2):** valid formats, invalid → NULL
- **TestCleanOrders (6):** renames and types, profit rounding, multiple lines per order kept, duplicate Row ID removed, invalid rows filtered, bad date → NULL
- **TestCleanCustomers (5):** standardisation, phone cleaning, address flattening, duplicates / null IDs, NFC normalisation
- **TestCleanProducts (3):** columns and types, deterministic duplicate collapse, null ID filtered
- **TestBuildEnrichedCustomer (2):** totals per customer, customer with no orders
- **TestBuildEnrichedProduct (1):** totals per product
- **TestBuildEnrichedOrders (5):** enrichment and columns, 2-decimal profit, no join fan-out, Unknown for missing references, NULL date → NULL year
- **TestBuildAggregateProfit (4):** grain and sums, distinct order count, reconciliation with the fact table, SQL rollups

37 tests in total (10 test classes).

### Running the tests
Run `notebooks/tests/test_transformations.py` as a notebook on a cluster. The first cells `%run` the `utils` and `transformations` notebooks, so the workspace paths at the top must match your workspace.

Each test prints its own result:
```
Test Passed : clean_orders: renames and types
Test Failed : clean_orders: bad date -> NULL -> AssertionError(...)
```
The last cell prints `N passed, M failed, 37 total` and raises an error if `M > 0`, so a scheduled job running this notebook fails when a test fails.

---

## Author
Santosh Kumar Shaw

## AI Statement

AI tools were used to assist with documentation, debugging schema issues and drafting test cases. The transformations and business rules were reviewed against the provided datasets.
