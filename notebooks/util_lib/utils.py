# Databricks notebook source

# MAGIC %md
# MAGIC # Utility Functions
# MAGIC
# MAGIC This notebook contains helper functions for date parsing and text normalization.

# COMMAND ----------

import unicodedata
import re
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql import Column
from pyspark.sql.types import StringType as _ST

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Parsing Functions

# COMMAND ----------

_DATE_FORMATS = [
    "dd/MM/yyyy",          # Added for Orders.json format (21/8/2016)
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "yyyy.MM.dd",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "dd-MMM-yyyy",
    "dd-MMMM-yyyy",
    "MM/dd/yyyy",
    "dd-MM-yyyy",
    "MMMM dd, yyyy"
]

# COMMAND ----------

def parse_date_col(col_name: str) -> Column:
    """
    Attempts to parse a column containing mixed timestamp/date formats.

    Tries multiple formats sequentially and returns the first successfully
    parsed date using coalesce. Invalid or non-matching values safely return NULL.
    """
    result = F.lit(None).cast("date")
    for fmt in reversed(_DATE_FORMATS):
        result = F.coalesce(
            F.to_date(F.trim(F.col(col_name)), fmt),
            result
        )
    return result

# COMMAND ----------

def parse_timestamp_col(col_name: str) -> Column:
    """
    Attempts to parse a column containing mixed timestamp/date formats.

    Tries multiple formats sequentially and returns the first successfully
    parsed timestamp using coalesce. Invalid or non-matching values safely return NULL.
    """
    ts_formats = [
        "dd/MM/yyyy HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd",
        "yyyy/MM/dd",
        "yyyy.MM.dd",
        "dd/MM/yyyy",
    ]
    result = F.lit(None).cast("timestamp")
    for fmt in reversed(ts_formats):
        result = F.coalesce(
            F.to_timestamp(F.trim(F.col(col_name)), fmt),
            result
        )
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unicode Normalization Functions

# COMMAND ----------

def normalize_unicode(text: Optional[str]) -> Optional[str]:
    """
    Normalizes Unicode text using NFC (Normalization Form Canonical Composition).
    Ensures consistent representation of characters across the dataset.
    """
    if text is None:
        return None
    return unicodedata.normalize("NFC", text)

# COMMAND ----------

def get_normalize_udf():
    """Returns a PySpark UDF for Unicode normalization"""
    from pyspark.sql.functions import udf
    return udf(normalize_unicode, _ST())
