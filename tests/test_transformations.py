import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType, DateType
)


import sys, os
sys.path.insert(0, "../assignment/notebooks/util_lib/")

from transformations import (
    clean_jobs,
    clean_candidates,
    clean_education,
    clean_applications,
    clean_workflow_events,
    build_fact_applications,
    build_dim_candidate,
    compute_time_to_hire,
)
from utils import parse_date_col


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("ats_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# parse_date_col
# ---------------------------------------------------------------------------

class TestParseDateCol:

    def test_iso_format(self, spark):
        df = spark.createDataFrame([("2025-03-12",)], ["d"])
        result = df.withColumn("parsed", parse_date_col("d")).collect()[0]["parsed"]
        assert result == date(2025, 3, 12)

    def test_slash_format(self, spark):
        df = spark.createDataFrame([("2025/07/25",)], ["d"])
        result = df.withColumn("parsed", parse_date_col("d")).collect()[0]["parsed"]
        assert result == date(2025, 7, 25)

    def test_dot_format(self, spark):
        df = spark.createDataFrame([("2025.01.01",)], ["d"])
        result = df.withColumn("parsed", parse_date_col("d")).collect()[0]["parsed"]
        assert result == date(2025, 1, 1)

    def test_null_input(self, spark):
        df = spark.createDataFrame([(None,)], ["d"])
        result = df.withColumn("parsed", parse_date_col("d")).collect()[0]["parsed"]
        assert result is None

    def test_invalid_string_returns_null(self, spark):
        df = spark.createDataFrame([("not-a-date",)], ["d"])
        result = df.withColumn("parsed", parse_date_col("d")).collect()[0]["parsed"]
        assert result is None


# ---------------------------------------------------------------------------
# clean_jobs
# ---------------------------------------------------------------------------

class TestCleanJobs:

    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("job_id",      StringType(), True),
            StructField("title",       StringType(), True),
            StructField("department",  StringType(), True),
            StructField("posted_date", StringType(), True),
            StructField("status",      StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def test_deduplicates_on_job_id(self, spark):
        df = self._make_df(spark, [
            ("j1", "Eng", "Tech", "2025-01-01", "open"),
            ("j1", "Eng", "Tech", "2025-01-01", "open"),   # duplicate
        ])
        result = clean_jobs(df)
        assert result.count() == 1

    def test_status_title_cased(self, spark):
        df = self._make_df(spark, [("j1", "Eng", "Tech", "2025-01-01", "OPEN")])
        result = clean_jobs(df).collect()[0]
        assert result["status"] == "Open"

    def test_mixed_date_formats_parsed(self, spark):
        rows = [
            ("j1", "A", "D", "2025-01-01", "Open"),
            ("j2", "B", "D", "2025/06/15", "Open"),
            ("j3", "C", "D", "2025.03.20", "Closed"),
        ]
        df = self._make_df(spark, rows)
        result = {r["job_id"]: r["posted_date"] for r in clean_jobs(df).collect()}
        assert result["j1"] == date(2025, 1, 1)
        assert result["j2"] == date(2025, 6, 15)
        assert result["j3"] == date(2025, 3, 20)

    def test_unicode_preserved(self, spark):
        df = self._make_df(spark, [("j1", "Ingénieur", "R&D", "2025-01-01", "Open")])
        result = clean_jobs(df).collect()[0]
        assert "Ingénieur" in result["title"]

    def test_whitespace_trimmed(self, spark):
        df = self._make_df(spark, [("j1", "  Engineer  ", " Tech ", "2025-01-01", "Open")])
        result = clean_jobs(df).collect()[0]
        assert result["title"] == "Engineer"
        assert result["department"] == "Tech"


# ---------------------------------------------------------------------------
# clean_candidates
# ---------------------------------------------------------------------------

class TestCleanCandidates:

    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("candidate_id", StringType(), True),
            StructField("first_name",   StringType(), True),
            StructField("last_name",    StringType(), True),
            StructField("email",        StringType(), True),
            StructField("phone",        StringType(), True),
            StructField("skills",       ArrayType(StringType()), True),
        ])
        return spark.createDataFrame(rows, schema)

    def test_dedup_on_candidate_id(self, spark):
        rows = [
            ("c1", "Alice", "A", "a@x.com", "1234567890", ["Python"]),
            ("c1", "Alice", "A", "a@x.com", "1234567890", ["Python"]),
        ]
        df = self._make_df(spark, rows)
        assert clean_candidates(df).count() == 1

    def test_email_lowercased(self, spark):
        rows = [("c1", "Alice", "A", "ALICE@X.COM", "1234567890", None)]
        df = self._make_df(spark, rows)
        result = clean_candidates(df).collect()[0]
        assert result["email"] == "alice@x.com"

    def test_phone_digits_only(self, spark):
        rows = [("c1", "Alice", "A", "a@x.com", "(836) 754-1458", None)]
        df = self._make_df(spark, rows)
        result = clean_candidates(df).collect()[0]
        assert result["phone"] == "8367541458"

    def test_unicode_name_preserved(self, spark):
        rows = [("c1", "José", "García", "j@x.com", "1234567890", None)]
        df = self._make_df(spark, rows)
        result = clean_candidates(df).collect()[0]
        assert result["first_name"] == "José"


# ---------------------------------------------------------------------------
# clean_education
# ---------------------------------------------------------------------------

class TestCleanEducation:

    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("candidate_id", StringType(),  True),
            StructField("degree",       StringType(),  True),
            StructField("institution",  StringType(),  True),
            StructField("year",         StringType(),  True),
        ])
        return spark.createDataFrame(rows, schema)

    def test_year_cast_to_int(self, spark):
        df = self._make_df(spark, [("c1", "BS", "MIT", "2019")])
        result = clean_education(df).collect()[0]
        assert result["year"] == 2019
        assert isinstance(result["year"], int)

    def test_implausible_year_nullified(self, spark):
        df = self._make_df(spark, [("c1", "BS", "MIT", "1800")])
        result = clean_education(df).collect()[0]
        assert result["year"] is None

    def test_dedup(self, spark):
        rows = [
            ("c1", "BS", "MIT", "2019"),
            ("c1", "BS", "MIT", "2019"),
        ]
        df = self._make_df(spark, rows)
        assert clean_education(df).count() == 1


# ---------------------------------------------------------------------------
# clean_workflow_events
# ---------------------------------------------------------------------------

class TestCleanWorkflowEvents:

    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("application_id",  StringType(), True),
            StructField("old_status",      StringType(), True),
            StructField("new_status",      StringType(), True),
            StructField("event_timestamp", StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def test_event_id_generated(self, spark):
        df = self._make_df(spark, [("app1", None, "Applied", "2025-03-12")])
        result = clean_workflow_events(df).collect()[0]
        assert result["event_id"] is not None
        assert len(result["event_id"]) == 32   # md5 hex

    def test_null_application_id_dropped(self, spark):
        rows = [
            (None,   None, "Applied", "2025-03-12"),
            ("app1", None, "Applied", "2025-03-12"),
        ]
        df = self._make_df(spark, rows)
        assert clean_workflow_events(df).count() == 1

    def test_duplicate_events_deduped(self, spark):
        rows = [
            ("app1", None, "Applied", "2025-03-12"),
            ("app1", None, "Applied", "2025-03-12"),
        ]
        df = self._make_df(spark, rows)
        assert clean_workflow_events(df).count() == 1


# ---------------------------------------------------------------------------
# build_fact_applications
# ---------------------------------------------------------------------------

class TestBuildFctApplications:

    def test_is_hired_true_when_hired_event_exists(self, spark):
        from pyspark.sql.types import TimestampType, BooleanType
        from datetime import datetime

        apps_schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("job_id",         StringType(), True),
            StructField("candidate_id",   StringType(), True),
            StructField("apply_date",     DateType(),   True),
        ])
        apps = spark.createDataFrame(
            [("app1", "job1", "cand1", date(2025, 1, 1))],
            apps_schema
        )

        wf_schema = StructType([
            StructField("event_id",        StringType(),  True),
            StructField("application_id",  StringType(),  True),
            StructField("old_status",      StringType(),  True),
            StructField("new_status",      StringType(),  True),
            StructField("event_timestamp", StringType(),  True),
            StructField("anomaly_flag",    BooleanType(), True),
            StructField("anomaly_reason",  StringType(),  True),
        ])
        events = spark.createDataFrame([
            ("e1", "app1", None,       "Applied", "2025-01-01", False, None),
            ("e2", "app1", "Applied",  "Hired",   "2025-02-01", False, None),
        ], wf_schema).withColumn(
            "event_timestamp", F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd")
        )

        result = build_fact_applications(apps, events).collect()[0]
        assert result["is_hired"] is True
        assert result["current_status"] == "Hired"

    def test_is_hired_false_when_no_hired_event(self, spark):
        from pyspark.sql.types import BooleanType

        apps_schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("job_id",         StringType(), True),
            StructField("candidate_id",   StringType(), True),
            StructField("apply_date",     DateType(),   True),
        ])
        apps = spark.createDataFrame(
            [("app2", "job1", "cand1", date(2025, 1, 1))],
            apps_schema
        )

        wf_schema = StructType([
            StructField("event_id",        StringType(),  True),
            StructField("application_id",  StringType(),  True),
            StructField("old_status",      StringType(),  True),
            StructField("new_status",      StringType(),  True),
            StructField("event_timestamp", StringType(),  True),
            StructField("anomaly_flag",    BooleanType(), True),
            StructField("anomaly_reason",  StringType(),  True),
        ])
        events = spark.createDataFrame([
            ("e1", "app2", None, "Applied", "2025-01-01", False, None),
        ], wf_schema).withColumn(
            "event_timestamp", F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd")
        )

        result = build_fact_applications(apps, events).collect()[0]
        assert result["is_hired"] is False
