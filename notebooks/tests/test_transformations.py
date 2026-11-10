import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, datetime
from decimal import Decimal

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/transformations

# COMMAND ----------

class TestCleanOrders(unittest.TestCase):
    """Unit tests for clean_orders transformation"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestCleanOrders").getOrCreate()

    def test_clean_orders_basic(self):
        """Test basic cleaning of orders data"""
        data = [
            ("O001", "C001", "P001", "2023-01-15", "5", "100.50", "60.25"),
            ("O002", "C002", "P002", "2023-02-20", "3", "200.00", "120.00"),
        ]
        df = self.spark.createDataFrame(data, ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "unit_cost"])

        result = clean_orders(df)

        self.assertEqual(result.count(), 2)
        self.assertEqual(result.filter(F.col("order_id") == "O001").first()["quantity"], 5)
        self.assertEqual(float(result.filter(F.col("order_id") == "O001").first()["unit_price"]), 100.50)

    def test_clean_orders_removes_duplicates(self):
        """Test that duplicate order_ids are removed"""
        data = [
            ("O001", "C001", "P001", "2023-01-15", "5", "100.50", "60.25"),
            ("O001", "C001", "P001", "2023-01-15", "5", "100.50", "60.25"),
            ("O002", "C002", "P002", "2023-02-20", "3", "200.00", "120.00"),
        ]
        df = self.spark.createDataFrame(data, ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "unit_cost"])

        result = clean_orders(df)

        self.assertEqual(result.count(), 2)

    def test_clean_orders_filters_invalid_records(self):
        """Test that invalid records are filtered out"""
        data = [
            ("O001", "C001", "P001", "2023-01-15", "5", "100.50", "60.25"),
            (None, "C002", "P002", "2023-02-20", "3", "200.00", "120.00"),  # null order_id
            ("O003", None, "P003", "2023-03-25", "2", "150.00", "90.00"),  # null customer_id
            ("O004", "C004", None, "2023-04-30", "4", "175.00", "105.00"),  # null product_id
            ("O005", "C005", "P005", "2023-05-15", "-1", "125.00", "75.00"),  # negative quantity
            ("O006", "C006", "P006", "2023-06-20", "2", "-50.00", "30.00"),  # negative price
        ]
        df = self.spark.createDataFrame(data, ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "unit_cost"])

        result = clean_orders(df)

        self.assertEqual(result.count(), 1)
        self.assertEqual(result.first()["order_id"], "O001")

    def test_clean_orders_date_parsing(self):
        """Test that dates are parsed correctly"""
        data = [
            ("O001", "C001", "P001", "2023-01-15", "5", "100.50", "60.25"),
            ("O002", "C002", "P002", "01/15/2023", "3", "200.00", "120.00"),
        ]
        df = self.spark.createDataFrame(data, ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "unit_cost"])

        result = clean_orders(df)

        self.assertTrue(isinstance(result.schema["order_date"].dataType, DateType))


class TestCleanCustomers(unittest.TestCase):
    """Unit tests for clean_customers transformation"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestCleanCustomers").getOrCreate()

    def test_clean_customers_basic(self):
        """Test basic cleaning of customers data"""
        data = [
            ("C001", "John Doe", "USA", "New York", "john@example.com"),
            ("C002", "Jane Smith", "UK", "London", "jane@example.com"),
        ]
        df = self.spark.createDataFrame(data, ["customer_id", "customer_name", "country", "city", "email"])

        result = clean_customers(df)

        self.assertEqual(result.count(), 2)
        self.assertTrue(result.filter(F.col("customer_id") == "C001").first()["email"].startswith("john"))

    def test_clean_customers_removes_duplicates(self):
        """Test that duplicate customer_ids are removed"""
        data = [
            ("C001", "John Doe", "USA", "New York", "john@example.com"),
            ("C001", "John Doe", "USA", "New York", "john@example.com"),
            ("C002", "Jane Smith", "UK", "London", "jane@example.com"),
        ]
        df = self.spark.createDataFrame(data, ["customer_id", "customer_name", "country", "city", "email"])

        result = clean_customers(df)

        self.assertEqual(result.count(), 2)

    def test_clean_customers_filters_null_id(self):
        """Test that null customer_ids are filtered out"""
        data = [
            ("C001", "John Doe", "USA", "New York", "john@example.com"),
            (None, "Jane Smith", "UK", "London", "jane@example.com"),
        ]
        df = self.spark.createDataFrame(data, ["customer_id", "customer_name", "country", "city", "email"])

        result = clean_customers(df)

        self.assertEqual(result.count(), 1)

    def test_clean_customers_lowercases_email(self):
        """Test that emails are lowercased"""
        data = [
            ("C001", "John Doe", "USA", "New York", "JOHN@EXAMPLE.COM"),
        ]
        df = self.spark.createDataFrame(data, ["customer_id", "customer_name", "country", "city", "email"])

        result = clean_customers(df)

        self.assertEqual(result.first()["email"], "john@example.com")


class TestCleanProducts(unittest.TestCase):
    """Unit tests for clean_products transformation"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestCleanProducts").getOrCreate()

    def test_clean_products_basic(self):
        """Test basic cleaning of products data"""
        data = [
            ("P001", "Laptop", "Electronics", "Computers"),
            ("P002", "Mouse", "Electronics", "Accessories"),
        ]
        df = self.spark.createDataFrame(data, ["product_id", "product_name", "category", "sub_category"])

        result = clean_products(df)

        self.assertEqual(result.count(), 2)

    def test_clean_products_removes_duplicates(self):
        """Test that duplicate product_ids are removed"""
        data = [
            ("P001", "Laptop", "Electronics", "Computers"),
            ("P001", "Laptop", "Electronics", "Computers"),
            ("P002", "Mouse", "Electronics", "Accessories"),
        ]
        df = self.spark.createDataFrame(data, ["product_id", "product_name", "category", "sub_category"])

        result = clean_products(df)

        self.assertEqual(result.count(), 2)

    def test_clean_products_filters_null_id(self):
        """Test that null product_ids are filtered out"""
        data = [
            ("P001", "Laptop", "Electronics", "Computers"),
            (None, "Mouse", "Electronics", "Accessories"),
        ]
        df = self.spark.createDataFrame(data, ["product_id", "product_name", "category", "sub_category"])

        result = clean_products(df)

        self.assertEqual(result.count(), 1)

    def test_clean_products_title_cases_categories(self):
        """Test that categories are title-cased"""
        data = [
            ("P001", "Laptop", "electronics", "computers"),
        ]
        df = self.spark.createDataFrame(data, ["product_id", "product_name", "category", "sub_category"])

        result = clean_products(df)

        self.assertEqual(result.first()["category"], "Electronics")
        self.assertEqual(result.first()["sub_category"], "Computers")


class TestBuildEnrichedCustomer(unittest.TestCase):
    """Unit tests for build_enriched_customer transformation"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestBuildEnrichedCustomer").getOrCreate()

    def test_build_enriched_customer_basic(self):
        """Test basic enrichment of customer data"""
        customers_data = [
            ("C001", "John Doe", "USA", "New York", "john@example.com"),
            ("C002", "Jane Smith", "UK", "London", "jane@example.com"),
        ]
        customers_df = self.spark.createDataFrame(customers_data, ["customer_id", "customer_name", "country", "city", "email"])

        orders_data = [
            ("O001", "C001", "P001", date(2023, 1, 15), 5, Decimal("100.50"), Decimal("60.25")),
            ("O002", "C001", "P002", date(2023, 2, 20), 3, Decimal("200.00"), Decimal("120.00")),
            ("O003", "C002", "P003", date(2023, 3, 25), 2, Decimal("150.00"), Decimal("90.00")),
        ]
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("unit_cost", DecimalType(10, 2), True),
        ])
        orders_df = self.spark.createDataFrame(orders_data, orders_schema)

        result = build_enriched_customer(customers_df, orders_df)

        c001_row = result.filter(F.col("customer_id") == "C001").first()
        self.assertEqual(c001_row["total_orders"], 2)
        self.assertEqual(float(c001_row["total_revenue"]), 1102.50)

    def test_build_enriched_customer_no_orders(self):
        """Test customer with no orders"""
        customers_data = [
            ("C001", "John Doe", "USA", "New York", "john@example.com"),
        ]
        customers_df = self.spark.createDataFrame(customers_data, ["customer_id", "customer_name", "country", "city", "email"])

        orders_data = []
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("unit_cost", DecimalType(10, 2), True),
        ])
        orders_df = self.spark.createDataFrame(orders_data, orders_schema)

        result = build_enriched_customer(customers_df, orders_df)

        c001_row = result.filter(F.col("customer_id") == "C001").first()
        self.assertEqual(c001_row["total_orders"], 0)
        self.assertEqual(float(c001_row["total_revenue"]), 0.0)


class TestBuildEnrichedOrders(unittest.TestCase):
    """Unit tests for build_enriched_orders transformation"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestBuildEnrichedOrders").getOrCreate()

    def test_build_enriched_orders_basic(self):
        """Test basic enrichment of orders data"""
        orders_data = [
            ("O001", "C001", "P001", date(2023, 1, 15), 5, Decimal("100.50"), Decimal("60.25")),
        ]
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("unit_cost", DecimalType(10, 2), True),
        ])
        orders_df = self.spark.createDataFrame(orders_data, orders_schema)

        customers_data = [
            ("C001", "John Doe", "USA", "New York", "john@example.com"),
        ]
        customers_df = self.spark.createDataFrame(customers_data, ["customer_id", "customer_name", "country", "city", "email"])

        products_data = [
            ("P001", "Laptop", "Electronics", "Computers"),
        ]
        products_df = self.spark.createDataFrame(products_data, ["product_id", "product_name", "category", "sub_category"])

        result = build_enriched_orders(orders_df, customers_df, products_df)

        row = result.first()
        self.assertEqual(row["order_id"], "O001")
        self.assertEqual(row["customer_name"], "John Doe")
        self.assertEqual(row["country"], "USA")
        self.assertEqual(row["category"], "Electronics")
        self.assertEqual(row["sub_category"], "Computers")
        self.assertEqual(float(row["profit"]), 201.25)  # (100.50 * 5) - (60.25 * 5)

    def test_build_enriched_orders_profit_calculation(self):
        """Test profit calculation"""
        orders_data = [
            ("O001", "C001", "P001", date(2023, 1, 15), 10, Decimal("50.00"), Decimal("30.00")),
        ]
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("unit_cost", DecimalType(10, 2), True),
        ])
        orders_df = self.spark.createDataFrame(orders_data, orders_schema)

        customers_data = [("C001", "John Doe", "USA", "New York", "john@example.com")]
        customers_df = self.spark.createDataFrame(customers_data, ["customer_id", "customer_name", "country", "city", "email"])

        products_data = [("P001", "Laptop", "Electronics", "Computers")]
        products_df = self.spark.createDataFrame(products_data, ["product_id", "product_name", "category", "sub_category"])

        result = build_enriched_orders(orders_df, customers_df, products_df)

        row = result.first()
        self.assertEqual(float(row["revenue"]), 500.00)
        self.assertEqual(float(row["cost"]), 300.00)
        self.assertEqual(float(row["profit"]), 200.00)

    def test_build_enriched_orders_year_extraction(self):
        """Test year extraction from order_date"""
        orders_data = [
            ("O001", "C001", "P001", date(2023, 1, 15), 5, Decimal("100.00"), Decimal("60.00")),
        ]
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("unit_cost", DecimalType(10, 2), True),
        ])
        orders_df = self.spark.createDataFrame(orders_data, orders_schema)

        customers_data = [("C001", "John Doe", "USA", "New York", "john@example.com")]
        customers_df = self.spark.createDataFrame(customers_data, ["customer_id", "customer_name", "country", "city", "email"])

        products_data = [("P001", "Laptop", "Electronics", "Computers")]
        products_df = self.spark.createDataFrame(products_data, ["product_id", "product_name", "category", "sub_category"])

        result = build_enriched_orders(orders_df, customers_df, products_df)

        self.assertEqual(result.first()["year"], 2023)


class TestBuildAggregateProfit(unittest.TestCase):
    """Unit tests for build_aggregate_profit transformation"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestBuildAggregateProfit").getOrCreate()

    def test_build_aggregate_profit_basic(self):
        """Test basic profit aggregation"""
        data = [
            ("O001", date(2023, 1, 15), 2023, "C001", "John Doe", "USA", "P001", "Laptop", "Electronics", "Computers",
             5, Decimal("100.00"), Decimal("60.00"), Decimal("500.00"), Decimal("300.00"), Decimal("200.00")),
            ("O002", date(2023, 2, 20), 2023, "C001", "John Doe", "USA", "P002", "Mouse", "Electronics", "Accessories",
             3, Decimal("50.00"), Decimal("30.00"), Decimal("150.00"), Decimal("90.00"), Decimal("60.00")),
        ]
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_date", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("country", StringType(), True),
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("unit_cost", DecimalType(10, 2), True),
            StructField("revenue", DecimalType(10, 2), True),
            StructField("cost", DecimalType(10, 2), True),
            StructField("profit", DecimalType(10, 2), True),
        ])
        fact_orders = self.spark.createDataFrame(data, schema)

        result = build_aggregate_profit(fact_orders)

        # Should aggregate by year, category, sub_category, customer
        aggregated = result.filter(
            (F.col("year") == 2023) &
            (F.col("category") == "Electronics") &
            (F.col("customer_id") == "C001")
        ).collect()

        self.assertEqual(len(aggregated), 2)  # Two sub-categories


# COMMAND ----------

# Run tests
if __name__ == "__main__":
    unittest.main(argv=[''], exit=False, verbosity=2)
