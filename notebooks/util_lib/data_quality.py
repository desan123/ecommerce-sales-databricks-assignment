from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class DQResult:
    check_name: str
    passed: bool
    message: str
    failed_count: int = 0
    sample_failures: Optional[List[dict]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_uniqueness(df: DataFrame, pk_cols: List[str], table_name: str) -> DQResult:
    """Asserts that the combination of pk_cols is unique across all rows."""
    total = df.count()
    distinct = df.select(*pk_cols).distinct().count()
    duplicates = total - distinct
    passed = duplicates == 0
    return DQResult(
        check_name=f"uniqueness:{table_name}:{'+'.join(pk_cols)}",
        passed=passed,
        message=f"{duplicates} duplicate PK rows found" if not passed else "OK",
        failed_count=duplicates,
    )


def check_not_null(df: DataFrame, cols: List[str], table_name: str) -> DQResult:
    """Asserts that specified columns contain no nulls."""
    null_counts = {
        c: df.filter(F.col(c).isNull()).count()
        for c in cols
    }
    failures = {c: n for c, n in null_counts.items() if n > 0}
    passed = len(failures) == 0
    return DQResult(
        check_name=f"not_null:{table_name}:{','.join(cols)}",
        passed=passed,
        message=str(failures) if not passed else "OK",
        failed_count=sum(failures.values()),
    )


def check_volume(
    df: DataFrame,
    table_name: str,
    min_rows: int,
    max_rows: int
) -> DQResult:
    """Flags if row count is outside the expected [min_rows, max_rows] band."""
    count = df.count()
    passed = min_rows <= count <= max_rows
    return DQResult(
        check_name=f"volume:{table_name}",
        passed=passed,
        message=f"Row count {count} outside [{min_rows}, {max_rows}]" if not passed else f"OK ({count} rows)",
        failed_count=0 if passed else 1,
    )


def check_referential_integrity(
    child_df: DataFrame,
    fk_col: str,
    parent_df: DataFrame,
    pk_col: str,
    child_table: str,
    parent_table: str,
) -> DQResult:
    """
    Checks that every non-null FK value in child_df exists in parent_df.
    """
    orphans = (
        child_df
        .filter(F.col(fk_col).isNotNull())
        .join(parent_df.select(pk_col), child_df[fk_col] == parent_df[pk_col], how="left_anti")
    )
    count = orphans.count()
    passed = count == 0
    return DQResult(
        check_name=f"ref_integrity:{child_table}.{fk_col} is {parent_table}.{pk_col}",
        passed=passed,
        message=f"{count} orphan FK rows" if not passed else "OK",
        failed_count=count,
    )


# ---------------------------------------------------------------------------
# Anomaly detection: Hired-before-Applied
# ---------------------------------------------------------------------------

def detect_hired_before_applied(workflow_df: DataFrame) -> DataFrame:

    applied_ts = (
        workflow_df
        .filter(F.upper(F.col("new_status")) == "APPLIED")
        .groupBy("application_id")
        .agg(F.min("event_timestamp").alias("applied_ts"))
    )

    hired_ts = (
        workflow_df
        .filter(F.upper(F.col("new_status")) == "HIRED")
        .groupBy("application_id")
        .agg(F.min("event_timestamp").alias("hired_ts"))
    )

    # Join both back to the full event stream
    joined_data = (
        workflow_df
        .join(applied_ts, on="application_id", how="left")
        .join(hired_ts,   on="application_id", how="left")
    )

    joined_data = joined_data.withColumn(
        "anomaly_flag",
        F.when(
            F.col("hired_ts").isNotNull() &
            F.col("applied_ts").isNotNull() &
            (F.col("hired_ts") < F.col("applied_ts")),
            F.lit(True)
        ).otherwise(F.lit(False))
    ).withColumn(
        "anomaly_reason",
        F.when(
            F.col("anomaly_flag") == True,
            F.concat_ws(" ",
                F.lit("Hired timestamp"),
                F.col("hired_ts").cast("string"),
                F.lit("precedes Applied timestamp"),
                F.col("applied_ts").cast("string"),
            )
        ).otherwise(F.lit(None).cast("string"))
    ).drop("applied_ts", "hired_ts")

    return joined_data


# ---------------------------------------------------------------------------
# Run all checks and return a summary DataFrame
# ---------------------------------------------------------------------------

def run_dq_suite(
    spark: SparkSession,
    results: List[DQResult],
    suite_name: str = "ats_dq",
) -> DataFrame:
    """
    Converts a list of DQResult objects into a Spark DataFrame for logging.
    In production this would be written to a Delta audit table.
    """
    rows = [
        (
            suite_name,
            r.check_name,
            r.passed,
            r.message,
            r.failed_count,
            datetime.utcnow().isoformat(),
        )
        for r in results
    ]
    return spark.createDataFrame(
        rows,
        schema="suite_name STRING, check_name STRING, passed BOOLEAN, "
               "message STRING, failed_count INT, run_timestamp STRING"
    )

# ---------------------------------------------------------------------------
# Print the Result
# ---------------------------------------------------------------------------

def print_dq_summary(results: List[DQResult]):
       for r in results:
           print(f"[{'PASS' if r.passed else 'FAIL'}] {r.check_name}: {r.message}")