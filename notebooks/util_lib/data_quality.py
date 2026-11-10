# Databricks notebook source

# MAGIC %md
# MAGIC # Data Quality Functions
# MAGIC
# MAGIC This notebook contains data quality check functions for validation.

# COMMAND ----------

from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Result Container

# COMMAND ----------

@dataclass
class DQResult:
    check_name: str
    passed: bool
    message: str
    failed_count: int = 0
    sample_failures: Optional[List[dict]] = field(default_factory=list)

    @property
    def details(self) -> str:
        return self.message

# COMMAND ----------

# MAGIC %md
# MAGIC ## Individual Check Functions

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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
        check_name=f"ref_integrity:{child_table}.{fk_col} -> {parent_table}.{pk_col}",
        passed=passed,
        message=f"{count} orphan FK rows" if not passed else "OK",
        failed_count=count,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Suite Runner

# COMMAND ----------

def run_dq_suite(
    spark: SparkSession,
    results: List[DQResult],
    suite_name: str = "ecommerce_dq",
) -> DataFrame:
    """
    Converts a list of DQResult objects into a Spark DataFrame for logging.
    This DataFrame is written to a Delta audit table.
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

# COMMAND ----------

def print_dq_summary(results: List[DQResult]):
    """Pretty print DQ results"""
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"{status} | {r.check_name}: {r.message}")
