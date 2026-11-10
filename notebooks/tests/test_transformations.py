# Databricks notebook source

# MAGIC %md
# MAGIC # Unit tests for utilities and transformations
# MAGIC
# MAGIC Plain-Python tests (no unittest / pytest). Each test prints `Test Passed` or
# MAGIC `Test Failed`; the last cell runs everything and fails the notebook if any test failed.
# MAGIC
# MAGIC Inputs mirror the real Bronze layout (string columns, original column names).

# COMMAND ----------

# MAGIC %run /Workspace/Users/sks12101997@gmail.com/assignment/notebooks/util_lib/utils

# COMMAND ----------

# MAGIC %run /Workspace/Users/sks12101997@gmail.com/assignment/notebooks/util_lib/transformations

# COMMAND ----------

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, DecimalType
)

RESULTS = []   # (test name, passed, error)


def report(name, passed, error=None):
    """Print the outcome of one test and remember it for the final summary."""
    RESULTS.append((name, passed, error))
    if passed:
        print(f"Test Passed : {name}")
    else:
        print(f"Test Failed : {name} -> {error!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input builders

# COMMAND ----------

BRONZE_ORDER_COLS = ["Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
                     "Customer ID", "Product ID", "Quantity", "Price", "Discount", "Profit"]
BRONZE_CUSTOMER_COLS = ["Customer ID", "Customer Name", "email", "phone", "address",
                        "Segment", "Country", "City", "State", "Postal Code", "Region"]
BRONZE_PRODUCT_COLS = ["Product ID", "Category", "Sub-Category", "Product Name", "State", "Price per product"]


def bronze_order(row_id="1", order_id="O1", order_date="21/8/2016", ship_date="25/8/2016",
                 customer="C1", product="P1", qty="2", price="100.00", discount="0.1", profit="10.456"):
    return (row_id, order_id, order_date, ship_date, "Standard Class",
            customer, product, qty, price, discount, profit)


def bronze_customer(cid="C1", name="John Doe", email="JOHN@Example.com", phone="001-542-415-0246x314",
                    address="1 Main St\nTown, FL 12345", segment="consumer", country="United States",
                    city="Miami", state="Florida", postal="33101", region="South"):
    return (cid, name, email, phone, address, segment, country, city, state, postal, region)


def bronze_product(pid="P1", cat="furniture", sub="chairs", name="Chair", state="Ohio", price="81.882"):
    return (pid, cat, sub, name, state, price)


SILVER_ORDERS_SCHEMA_T = StructType([
    StructField("row_id", IntegerType()), StructField("order_id", StringType()),
    StructField("order_date", DateType()), StructField("customer_id", StringType()),
    StructField("product_id", StringType()), StructField("quantity", IntegerType()),
    StructField("price", DecimalType(10, 2)), StructField("discount", DecimalType(5, 2)),
    StructField("profit", DecimalType(10, 2)),
])


def silver_orders(rows):
    return spark.createDataFrame(rows, SILVER_ORDERS_SCHEMA_T)


def silver_customers(rows):
    return spark.createDataFrame(
        rows, "customer_id string, customer_name string, segment string, "
              "country string, city string, state string, region string")


def silver_products(rows):
    return spark.createDataFrame(
        rows, "product_id string, product_name string, category string, sub_category string")


def run_clean_orders(rows):
    return clean_orders(spark.createDataFrame(rows, BRONZE_ORDER_COLS))


def run_clean_customers(rows):
    return clean_customers(spark.createDataFrame(rows, BRONZE_CUSTOMER_COLS))


def run_clean_products(rows):
    return clean_products(spark.createDataFrame(rows, BRONZE_PRODUCT_COLS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## utils: normalize_unicode

# COMMAND ----------

class TestNormalizeUnicode:

    def test_none_returns_none(self):
        try:
            assert normalize_unicode(None) is None
        except Exception as e:
            report("normalize_unicode: None returns None", False, e)
        else:
            report("normalize_unicode: None returns None", True)

    def test_plain_ascii_unchanged(self):
        try:
            assert normalize_unicode("hello") == "hello"
        except Exception as e:
            report("normalize_unicode: plain ascii unchanged", False, e)
        else:
            report("normalize_unicode: plain ascii unchanged", True)

    def test_nfc_normalisation(self):
        decomposed = "Jose\u0301"    # José (NFD)
        try:
            normalised = normalize_unicode(decomposed)
            assert normalised == "José"
            assert len(normalised) == 4   # NFC: 4 chars
        except Exception as e:
            report("normalize_unicode: NFC normalisation", False, e)
        else:
            report("normalize_unicode: NFC normalisation", True)

    def test_chinese_characters_preserved(self):
        text = "数据工程"
        try:
            assert normalize_unicode(text) == text
        except Exception as e:
            report("normalize_unicode: chinese characters preserved", False, e)
        else:
            report("normalize_unicode: chinese characters preserved", True)

    def test_empty_string(self):
        try:
            assert normalize_unicode("") == ""
        except Exception as e:
            report("normalize_unicode: empty string", False, e)
        else:
            report("normalize_unicode: empty string", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## utils: parse_date_col

# COMMAND ----------

class TestParseDateCol:

    @staticmethod
    def parse(value):
        df = spark.createDataFrame([(value,)], "d string")
        return df.select(parse_date_col("d").alias("r")).first()["r"]

    def test_day_month_year_single_digits(self):
        try:
            assert self.parse("21/8/2016") == date(2016, 8, 21)
        except Exception as e:
            report("parse_date_col: d/M/yyyy", False, e)
        else:
            report("parse_date_col: d/M/yyyy", True)

    def test_day_month_has_priority_over_month_day(self):
        try:
            assert self.parse("6/10/2016") == date(2016, 10, 6)
        except Exception as e:
            report("parse_date_col: d/M/yyyy has priority", False, e)
        else:
            report("parse_date_col: d/M/yyyy has priority", True)

    def test_iso_format(self):
        try:
            assert self.parse("2016-08-21") == date(2016, 8, 21)
        except Exception as e:
            report("parse_date_col: yyyy-MM-dd", False, e)
        else:
            report("parse_date_col: yyyy-MM-dd", True)

    def test_invalid_returns_null(self):
        try:
            assert self.parse("not a date") is None
            assert self.parse(None) is None
        except Exception as e:
            report("parse_date_col: invalid/None -> NULL", False, e)
        else:
            report("parse_date_col: invalid/None -> NULL", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## utils: clean_phone_col

# COMMAND ----------

class TestCleanPhoneCol:

    @staticmethod
    def clean(value):
        df = spark.createDataFrame([(value,)], "p string")
        return df.select(clean_phone_col("p").alias("r")).first()["r"]

    def test_valid_formats(self):
        cases = {
            "001-542-415-0246x314": "5424150246",
            "(674)865-6806x731": "6748656806",
            "718.871.8092x916": "7188718092",
            "7185624866": "7185624866",
        }
        try:
            for raw, expected in cases.items():
                assert self.clean(raw) == expected, f"{raw} -> {self.clean(raw)}"
        except Exception as e:
            report("clean_phone_col: valid formats", False, e)
        else:
            report("clean_phone_col: valid formats", True)

    def test_invalid_returns_null(self):
        try:
            assert self.clean("invalid") is None
            assert self.clean("12345") is None
            assert self.clean(None) is None
        except Exception as e:
            report("clean_phone_col: invalid -> NULL", False, e)
        else:
            report("clean_phone_col: invalid -> NULL", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## clean_orders

# COMMAND ----------

class TestCleanOrders:

    def test_renames_and_types(self):
        try:
            result = run_clean_orders([bronze_order()])
            assert result.columns == ["row_id", "order_id", "order_date", "ship_date", "ship_mode",
                                      "customer_id", "product_id", "quantity", "price", "discount", "profit"]
            row = result.first()
            assert row["row_id"] == 1
            assert row["quantity"] == 2
            assert row["order_date"] == date(2016, 8, 21)
            assert row["ship_date"] == date(2016, 8, 25)
            assert row["price"] == Decimal("100.00")
        except Exception as e:
            report("clean_orders: renames and types", False, e)
        else:
            report("clean_orders: renames and types", True)

    def test_profit_rounded_to_two_decimals(self):
        cases = {"10.456": Decimal("10.46"), "-14.924": Decimal("-14.92"), "0": Decimal("0.00")}
        try:
            for raw, expected in cases.items():
                assert run_clean_orders([bronze_order(profit=raw)]).first()["profit"] == expected
        except Exception as e:
            report("clean_orders: profit rounded to 2 decimals", False, e)
        else:
            report("clean_orders: profit rounded to 2 decimals", True)

    def test_same_order_id_multiple_lines_are_kept(self):
        try:
            result = run_clean_orders([bronze_order(row_id="1", order_id="O1", product="P1"),
                                       bronze_order(row_id="2", order_id="O1", product="P2")])
            assert result.count() == 2   # Order ID is not unique, Row ID is
        except Exception as e:
            report("clean_orders: multiple lines per order kept", False, e)
        else:
            report("clean_orders: multiple lines per order kept", True)

    def test_duplicate_row_id_removed(self):
        try:
            assert run_clean_orders([bronze_order(row_id="1"), bronze_order(row_id="1")]).count() == 1
        except Exception as e:
            report("clean_orders: duplicate row_id removed", False, e)
        else:
            report("clean_orders: duplicate row_id removed", True)

    def test_invalid_records_filtered(self):
        bad_cases = [dict(order_id=None), dict(customer=None), dict(product=None),
                     dict(qty="0"), dict(qty="-1"), dict(qty=None)]
        try:
            for bad in bad_cases:
                result = run_clean_orders([bronze_order(row_id="1"), bronze_order(row_id="2", **bad)])
                assert [r["row_id"] for r in result.collect()] == [1], f"not filtered: {bad}"
        except Exception as e:
            report("clean_orders: invalid records filtered", False, e)
        else:
            report("clean_orders: invalid records filtered", True)

    def test_unparseable_date_becomes_null(self):
        try:
            assert run_clean_orders([bronze_order(order_date="not a date")]).first()["order_date"] is None
        except Exception as e:
            report("clean_orders: bad date -> NULL", False, e)
        else:
            report("clean_orders: bad date -> NULL", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## clean_customers

# COMMAND ----------

class TestCleanCustomers:

    def test_standardisation(self):
        try:
            row = run_clean_customers([bronze_customer(name="  John Doe ", segment=" consumer ")]).first()
            assert row["customer_name"] == "John Doe"
            assert row["segment"] == "Consumer"
            assert row["email"] == "john@example.com"
            assert row["postal_code"] == "33101"
        except Exception as e:
            report("clean_customers: standardisation", False, e)
        else:
            report("clean_customers: standardisation", True)

    def test_phone_cleaned(self):
        try:
            assert run_clean_customers([bronze_customer(phone="001-542-415-0246x314")]).first()["phone"] == "5424150246"
            assert run_clean_customers([bronze_customer(phone="invalid")]).first()["phone"] is None
        except Exception as e:
            report("clean_customers: phone cleaned", False, e)
        else:
            report("clean_customers: phone cleaned", True)

    def test_address_newline_flattened(self):
        try:
            row = run_clean_customers([bronze_customer(address="1 Main St\nTown, FL 12345")]).first()
            assert row["address"] == "1 Main St, Town, FL 12345"
        except Exception as e:
            report("clean_customers: address flattened", False, e)
        else:
            report("clean_customers: address flattened", True)

    def test_duplicates_and_null_ids(self):
        try:
            result = run_clean_customers([bronze_customer(cid="C1"), bronze_customer(cid="C1"),
                                          bronze_customer(cid=None)])
            assert result.count() == 1
        except Exception as e:
            report("clean_customers: duplicates and null ids", False, e)
        else:
            report("clean_customers: duplicates and null ids", True)

    def test_unicode_normalised_to_nfc(self):
        try:
            row = run_clean_customers([bronze_customer(name="Jose\u0301")]).first()
            assert row["customer_name"] == "Jos\u00e9"
        except Exception as e:
            report("clean_customers: NFC normalisation", False, e)
        else:
            report("clean_customers: NFC normalisation", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## clean_products

# COMMAND ----------

class TestCleanProducts:

    def test_columns_and_types(self):
        try:
            result = run_clean_products([bronze_product()])
            assert result.columns == ["product_id", "category", "sub_category", "product_name", "state", "unit_price"]
            row = result.first()
            assert row["category"] == "Furniture"
            assert row["sub_category"] == "Chairs"
            assert row["unit_price"] == Decimal("81.88")
        except Exception as e:
            report("clean_products: columns and types", False, e)
        else:
            report("clean_products: columns and types", True)

    def test_duplicate_product_id_collapses_deterministically(self):
        try:
            result = run_clean_products([bronze_product(state="Pennsylvania", name="B"),
                                         bronze_product(state="New York", name="A")])
            assert result.count() == 1
            assert result.first()["state"] == "New York"
        except Exception as e:
            report("clean_products: duplicate product_id collapses", False, e)
        else:
            report("clean_products: duplicate product_id collapses", True)

    def test_null_product_id_filtered(self):
        try:
            assert run_clean_products([bronze_product(), bronze_product(pid=None)]).count() == 1
        except Exception as e:
            report("clean_products: null product_id filtered", False, e)
        else:
            report("clean_products: null product_id filtered", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## build_enriched_customer / build_enriched_product

# COMMAND ----------

class TestBuildEnrichedCustomer:

    def test_totals_per_customer(self):
        try:
            customers = silver_customers([("C1", "John", "Consumer", "US", "Miami", "FL", "South"),
                                          ("C2", "Jane", "Corporate", "US", "Austin", "TX", "South")])
            orders = silver_orders([
                (1, "O1", date(2016, 1, 1), "C1", "P1", 1, Decimal("100.00"), Decimal("0"), Decimal("10.50")),
                (2, "O1", date(2016, 1, 1), "C1", "P2", 1, Decimal("50.25"), Decimal("0"), Decimal("5.25")),
            ])
            rows = {r["customer_id"]: r for r in build_enriched_customer(customers, orders).collect()}
            assert rows["C1"]["total_orders"] == 2
            assert float(rows["C1"]["total_revenue"]) == 150.25
            assert float(rows["C1"]["total_profit"]) == 15.75
        except Exception as e:
            report("build_enriched_customer: totals per customer", False, e)
        else:
            report("build_enriched_customer: totals per customer", True)

    def test_customer_without_orders_has_zeros(self):
        try:
            customers = silver_customers([("C2", "Jane", "Corporate", "US", "Austin", "TX", "South")])
            row = build_enriched_customer(customers, silver_orders([])).first()
            assert row["total_orders"] == 0
            assert float(row["total_revenue"]) == 0.0
            assert float(row["total_profit"]) == 0.0
        except Exception as e:
            report("build_enriched_customer: no orders -> zeros", False, e)
        else:
            report("build_enriched_customer: no orders -> zeros", True)


class TestBuildEnrichedProduct:

    def test_totals_per_product(self):
        try:
            products = silver_products([("P1", "Chair", "Furniture", "Chairs"), ("P2", "Pen", "Office", "Art")])
            orders = silver_orders([
                (1, "O1", date(2016, 1, 1), "C1", "P1", 3, Decimal("30.00"), Decimal("0"), Decimal("3.00")),
                (2, "O2", date(2016, 1, 2), "C2", "P1", 2, Decimal("20.00"), Decimal("0"), Decimal("-1.00")),
            ])
            rows = {r["product_id"]: r for r in build_enriched_product(products, orders).collect()}
            assert rows["P1"]["total_quantity_sold"] == 5
            assert float(rows["P1"]["total_revenue"]) == 50.0
            assert float(rows["P1"]["total_profit"]) == 2.0
            assert rows["P2"]["total_quantity_sold"] == 0
        except Exception as e:
            report("build_enriched_product: totals per product", False, e)
        else:
            report("build_enriched_product: totals per product", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## build_enriched_orders

# COMMAND ----------

class TestBuildEnrichedOrders:

    @staticmethod
    def dims():
        customers = silver_customers([("C1", "John", "Consumer", "United States", "Miami", "FL", "South")])
        products = silver_products([("P1", "Chair", "Furniture", "Chairs")])
        return customers, products

    def test_enrichment_and_columns(self):
        try:
            orders = silver_orders([(1, "O1", date(2016, 8, 21), "C1", "P1", 2,
                                     Decimal("100.00"), Decimal("0.10"), Decimal("12.34"))])
            result = build_enriched_orders(orders, *self.dims())
            assert result.columns == ["row_id", "order_id", "order_date", "year", "month",
                                      "customer_id", "customer_name", "country", "region", "segment",
                                      "product_id", "product_name", "category", "sub_category",
                                      "quantity", "price", "discount", "profit"]
            row = result.first()
            assert row["customer_name"] == "John"
            assert row["country"] == "United States"
            assert row["category"] == "Furniture"
            assert row["sub_category"] == "Chairs"
            assert (row["year"], row["month"]) == (2016, 8)
            assert row["profit"] == Decimal("12.34")
        except Exception as e:
            report("build_enriched_orders: enrichment and columns", False, e)
        else:
            report("build_enriched_orders: enrichment and columns", True)

    def test_profit_has_two_decimals(self):
        try:
            orders = silver_orders([(1, "O1", date(2016, 8, 21), "C1", "P1", 2,
                                     Decimal("100.00"), Decimal("0.10"), Decimal("12.30"))])
            assert build_enriched_orders(orders, *self.dims()).schema["profit"].dataType.scale == 2
        except Exception as e:
            report("build_enriched_orders: profit has 2 decimals", False, e)
        else:
            report("build_enriched_orders: profit has 2 decimals", True)

    def test_row_count_preserved_no_fan_out(self):
        try:
            orders = silver_orders([
                (1, "O1", date(2016, 1, 1), "C1", "P1", 1, Decimal("1.00"), Decimal("0"), Decimal("1.00")),
                (2, "O1", date(2016, 1, 1), "C1", "P1", 1, Decimal("1.00"), Decimal("0"), Decimal("1.00")),
            ])
            assert build_enriched_orders(orders, *self.dims()).count() == 2
        except Exception as e:
            report("build_enriched_orders: no join fan-out", False, e)
        else:
            report("build_enriched_orders: no join fan-out", True)

    def test_missing_customer_or_product_kept_as_unknown(self):
        try:
            orders = silver_orders([(1, "O1", date(2016, 1, 1), "CX", "PX", 1,
                                     Decimal("1.00"), Decimal("0"), Decimal("1.00"))])
            row = build_enriched_orders(orders, *self.dims()).first()
            assert row["customer_name"] == "Unknown"
            assert row["category"] == "Unknown"
            assert row["sub_category"] == "Unknown"
        except Exception as e:
            report("build_enriched_orders: missing refs -> Unknown", False, e)
        else:
            report("build_enriched_orders: missing refs -> Unknown", True)

    def test_null_order_date_gives_null_year(self):
        try:
            orders = silver_orders([(1, "O1", None, "C1", "P1", 1,
                                     Decimal("1.00"), Decimal("0"), Decimal("1.00"))])
            assert build_enriched_orders(orders, *self.dims()).first()["year"] is None
        except Exception as e:
            report("build_enriched_orders: NULL date -> NULL year", False, e)
        else:
            report("build_enriched_orders: NULL date -> NULL year", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## build_aggregate_profit

# COMMAND ----------

class TestBuildAggregateProfit:

    @staticmethod
    def fact():
        customers = silver_customers([("C1", "John", "Consumer", "US", "Miami", "FL", "South"),
                                      ("C2", "Jane", "Consumer", "US", "Austin", "TX", "South")])
        products = silver_products([("P1", "Chair", "Furniture", "Chairs"), ("P2", "Pen", "Office", "Art")])
        orders = silver_orders([
            (1, "O1", date(2016, 1, 1), "C1", "P1", 1, Decimal("100.00"), Decimal("0.10"), Decimal("10.00")),
            (2, "O1", date(2016, 1, 1), "C1", "P1", 1, Decimal("50.00"), Decimal("0.20"), Decimal("5.50")),
            (3, "O1", date(2016, 1, 1), "C1", "P2", 1, Decimal("20.00"), Decimal("0.00"), Decimal("2.00")),
            (4, "O2", date(2017, 3, 1), "C1", "P1", 1, Decimal("10.00"), Decimal("0.00"), Decimal("1.00")),
            (5, "O3", date(2016, 5, 1), "C2", "P1", 1, Decimal("10.00"), Decimal("0.00"), Decimal("-3.00")),
        ])
        return build_enriched_orders(orders, customers, products)

    @staticmethod
    def chairs_2016_c1(agg):
        return agg.filter((F.col("year") == 2016) & (F.col("sub_category") == "Chairs")
                          & (F.col("customer_id") == "C1")).first()

    def test_grain_and_sums(self):
        try:
            agg = build_aggregate_profit(self.fact())
            assert agg.columns == ["year", "category", "sub_category", "customer_id", "customer_name",
                                   "total_profit", "total_revenue", "total_discount", "order_count"]
            row = self.chairs_2016_c1(agg)
            assert float(row["total_profit"]) == 15.50
            assert float(row["total_revenue"]) == 150.00
            assert float(row["total_discount"]) == 0.30
        except Exception as e:
            report("build_aggregate_profit: grain and sums", False, e)
        else:
            report("build_aggregate_profit: grain and sums", True)

    def test_order_count_is_distinct_orders(self):
        try:
            # two lines of order O1 in the same group count as one order
            assert self.chairs_2016_c1(build_aggregate_profit(self.fact()))["order_count"] == 1
        except Exception as e:
            report("build_aggregate_profit: distinct order count", False, e)
        else:
            report("build_aggregate_profit: distinct order count", True)

    def test_total_profit_reconciles_with_fact(self):
        try:
            fact = self.fact()
            agg = build_aggregate_profit(fact)
            assert agg.agg(F.sum("total_profit")).first()[0] == fact.agg(F.sum("profit")).first()[0]
        except Exception as e:
            report("build_aggregate_profit: reconciles with fact", False, e)
        else:
            report("build_aggregate_profit: reconciles with fact", True)

    def test_sql_rollups(self):
        try:
            build_aggregate_profit(self.fact()).createOrReplaceTempView("agg_profit_test")
            by_year = {r["year"]: float(r["p"]) for r in spark.sql(
                "SELECT year, ROUND(SUM(total_profit), 2) p FROM agg_profit_test GROUP BY year").collect()}
            assert by_year == {2016: 14.50, 2017: 1.00}
            by_customer = {r["customer_id"]: float(r["p"]) for r in spark.sql(
                "SELECT customer_id, ROUND(SUM(total_profit), 2) p FROM agg_profit_test GROUP BY customer_id").collect()}
            assert by_customer == {"C1": 18.50, "C2": -3.00}
        except Exception as e:
            report("build_aggregate_profit: SQL roll-ups", False, e)
        else:
            report("build_aggregate_profit: SQL roll-ups", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run all tests

# COMMAND ----------

TEST_CLASSES = [
    TestNormalizeUnicode, TestParseDateCol, TestCleanPhoneCol,
    TestCleanOrders, TestCleanCustomers, TestCleanProducts,
    TestBuildEnrichedCustomer, TestBuildEnrichedProduct,
    TestBuildEnrichedOrders, TestBuildAggregateProfit,
]

RESULTS.clear()
for cls in TEST_CLASSES:
    print(f"\n=== {cls.__name__} ===")
    instance = cls()
    for attr in sorted(dir(instance)):
        if attr.startswith("test_"):
            getattr(instance, attr)()

failed = [r for r in RESULTS if not r[1]]
print(f"\n{len(RESULTS) - len(failed)} passed, {len(failed)} failed, {len(RESULTS)} total")

# Fail the notebook (and any job running it) if a test failed
assert not failed, f"{len(failed)} test(s) failed: {[r[0] for r in failed]}"
