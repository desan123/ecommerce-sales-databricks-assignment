import pytest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType, DateType
)

# COMMAND ----------

# MAGIC %run /notebooks/util_lib/data_quality

# COMMAND ----------
# ---------------------------------------------------------------------------
# check_uniqueness
# ---------------------------------------------------------------------------

class TestCheckUniqueness:

    def test_passes_when_all_unique(self):
        df = spark.createDataFrame([("a",), ("b",), ("c",)], ["id"])
        result = check_uniqueness(df, ["id"], "test_table")
        try:
            assert result.passed is True
        except AssertionError as e:
            print("Uniqueness check :- Test Failed")
        else:
            print("Uniqueness check :- Test Passed")

    def test_fails_when_duplicates_exist(self):
        df = spark.createDataFrame([("a",), ("a",), ("b",)], ["id"])
        result = check_uniqueness(df, ["id"], "test_table")
        try:
            assert result.passed is False
            assert result.failed_count == 1
        except AssertionError as e:
            print("Uniqueness check :- Test Failed")
        else:
            print("Uniqueness check :- Test Passed")

    def test_composite_key(self):
        df = spark.createDataFrame(
            [("a", "x"), ("a", "y"), ("a", "x")],   # ("a","x") is a dup
            ["k1", "k2"]
        )
        result = check_uniqueness(df, ["k1", "k2"], "test")
        try:
            assert result.passed is False
            assert result.failed_count == 1
        except AssertionError as e:
            print("Uniqueness check :- Test Failed")
        else:
            print("Uniqueness check :- Test Passed")


# ---------------------------------------------------------------------------
# check_not_null
# ---------------------------------------------------------------------------

class TestCheckNotNull:

    def test_passes_no_nulls(self):
        df = spark.createDataFrame([("a", "x")], ["id", "val"])
        try:
            assert check_not_null(df, ["id", "val"], "t").passed is True
        except AssertionError as e:
            print("Not Null check :- Test Failed")
        else:
            print("Not Null check :- Test Passed")

    def test_fails_with_null(self):
        df = spark.createDataFrame([("a", None)], ["id", "val"])
        result = check_not_null(df, ["val"], "t")
        try:
            assert result.passed is False
            assert result.failed_count == 1
        except AssertionError as e:
            print("Not Null check :- Test Failed")
        else:
            print("Not Null check :- Test Passed")


# ---------------------------------------------------------------------------
# check_volume
# ---------------------------------------------------------------------------

class TestCheckVolume:

    def test_passes_within_range(self):
        df = spark.createDataFrame([("a",), ("b",)], ["id"])
        try:
            assert check_volume(df, "t", 1, 10).passed is True
        except AssertionError as e:
            print("Volumne check :- Test Failed")
        else:
            print("Volumne check :- Test Passed")

    def test_fails_below_min(self):
        df = spark.createDataFrame([("a",)], ["id"])
        try:
            assert check_volume(df, "t", 5, 10).passed is False
        except AssertionError as e:
            print("Volumne check :- Test Failed")
        else:
            print("Volumne check :- Test Passed")

    def test_fails_above_max(self):
        df = spark.createDataFrame([("a",), ("b",), ("c",)], ["id"])
        try:
            assert check_volume(df, "t", 1, 2).passed is False
        except AssertionError as e:
            print("Volumne check :- Test Failed")
        else:
            print("Volumne check :- Test Passed")



# ---------------------------------------------------------------------------
# check_referential_integrity
# ---------------------------------------------------------------------------

class TestRefIntegrity:

    def test_passes_all_fks_match(self):
        children = spark.createDataFrame([("a", "p1"), ("b", "p2")], ["id", "pid"])
        parents  = spark.createDataFrame([("p1",), ("p2",)], ["id"])
        result = check_referential_integrity(children, "pid", parents, "id", "child", "parent")
        try:
            assert result.passed is True
        except AssertionError as e:
            print("Referential Integrity check :- Test Failed")
        else:
            print("Referential Integrity check :- Test Passed")


    def test_fails_orphan_fk(self):
        children = spark.createDataFrame([("a", "p1"), ("b", "p99")], ["id", "pid"])
        parents  = spark.createDataFrame([("p1",)], ["id"])
        result = check_referential_integrity(children, "pid", parents, "id", "child", "parent")
        try:
            assert result.passed is False
            assert result.failed_count == 1
        except AssertionError as e:
            print("Referential Integrity check :- Test Failed")
        else:
            print("Referential Integrity check :- Test Passed")

    def test_null_fk_is_ignored(self):
        children = spark.createDataFrame([("a", None)], ["id", "pid"])
        parents  = spark.createDataFrame([("p1",)], ["id"])
        result = check_referential_integrity(children, "pid", parents, "id", "child", "parent")
        try:
            assert result.passed is True
        except AssertionError as e:
            print("Referential Integrity check :- Test Failed")
        else:
            print("Referential Integrity check :- Test Passed")


# ---------------------------------------------------------------------------
# detect_hired_before_applied
# ---------------------------------------------------------------------------

class TestDetectHiredBeforeApplied:

    def _make_workflow(self, rows):
        schema = StructType([
            StructField("event_id",       StringType(),    True),
            StructField("application_id", StringType(),    True),
            StructField("old_status",     StringType(),    True),
            StructField("new_status",     StringType(),    True),
            StructField("event_timestamp",TimestampType(), True),
        ])
        raw = spark.createDataFrame(
            [(r[0], r[1], r[2], r[3]) for r in rows],
            StructType([
                StructField("event_id",       StringType(), True),
                StructField("application_id", StringType(), True),
                StructField("old_status",     StringType(), True),
                StructField("new_status",     StringType(), True),
            ])
        )
        ts_col = [F.to_timestamp(F.lit(r[4]), "yyyy-MM-dd").alias("event_timestamp") for r in rows]
        # Build with timestamps
        rows_with_ts = [
            (r[0], r[1], r[2], r[3], r[4]) for r in rows
        ]
        return spark.createDataFrame(
            rows_with_ts,
            StructType([
                StructField("event_id",        StringType(), True),
                StructField("application_id",  StringType(), True),
                StructField("old_status",      StringType(), True),
                StructField("new_status",      StringType(), True),
                StructField("event_timestamp", StringType(), True),
            ])
        ).withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd"))

    def _make_applications(self, rows):
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("job_id",         StringType(), True),
            StructField("candidate_id",   StringType(), True),
            StructField("apply_date",     DateType(),   True),
        ])
        return spark.createDataFrame(
            [(r[0], "j1", "c1", date(2025, 1, 1)) for r in rows],
            schema
        )

    def test_normal_order_not_flagged(self):
        """Applied on Jan 1, Hired on Feb 1 — normal, no anomaly."""
        wf = self._make_workflow([
            ("e1", "app1", None,      "Applied", "2025-01-01"),
            ("e2", "app1", "Applied", "Hired",   "2025-02-01"),
        ])
        result = detect_hired_before_applied(wf)
        flags = {r["event_id"]: r["anomaly_flag"] for r in result.collect()}
        try:
            assert all(v is False for v in flags.values())
        except AssertionError as e:
            print("Hired Before Applied check :- Test Failed")
        else:
            print("Hired Before Applied check :- Test Passed")

    def test_reversed_order_flagged(self):
        """Hired on Jan 1, Applied on Feb 1 — anomaly for both events of this application."""
        wf = self._make_workflow([
            ("e1", "app2", None,   "Hired",   "2025-01-01"),   # hired first!
            ("e2", "app2", "Hired","Applied", "2025-02-01"),
        ])
        result = detect_hired_before_applied(wf)
        flags = {r["event_id"]: r["anomaly_flag"] for r in result.collect()}
        # All events for app2 should be flagged
        try:
            assert all(v is True for v in flags.values())
        except AssertionError as e:
            print("Hired Before Applied check :- Test Failed")
        else:
            print("Hired Before Applied check :- Test Passed")

    def test_anomaly_reason_populated(self):
        wf = self._make_workflow([
            ("e1", "app3", None,   "Hired",   "2025-01-01"),
            ("e2", "app3", "Hired","Applied", "2025-02-01"),
        ])
        result = detect_hired_before_applied(wf)
        for row in result.collect():
            try:
                assert row["anomaly_reason"] is not None
                assert "precedes" in row["anomaly_reason"]
            except AssertionError as e:
                print("Hired Before Applied check :- Test Failed")
            else:
                print("Hired Before Applied check :- Test Passed")

    def test_no_hired_event_not_flagged(self):
        """Application with no Hired event — should never be anomalous."""
        wf = self._make_workflow([
            ("e1", "app4", None,      "Applied",   "2025-01-01"),
            ("e2", "app4", "Applied", "Screening", "2025-01-10"),
        ])
        result = detect_hired_before_applied(wf)
        flags = {r["event_id"]: r["anomaly_flag"] for r in result.collect()}
        try:
            assert all(v is False for v in flags.values())
        except AssertionError as e:
            print("Hired Before Applied check :- Test Failed")
        else:
            print("Hired Before Applied check :- Test Passed")
