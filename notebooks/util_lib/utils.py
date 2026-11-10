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
    "d/M/yyyy",            # Added for Orders.json format (21/8/2016 - variable length day/month)
    "dd/MM/yyyy",          # Two-digit day and month (21/08/2016)
    "M/d/yyyy",            # US format variable length (8/21/2016)
    "MM/dd/yyyy",          # US format with leading zeros (08/21/2016)
    "yyyy-MM-dd",          # ISO format (2016-08-21)
    "yyyy/MM/dd",
    "yyyy.MM.dd",
    "dd-MMM-yyyy",
    "dd-MMMM-yyyy",
    "dd-MM-yyyy",
    "MMMM dd, yyyy"
]

# COMMAND ----------

def parse_date_col(col_name: str) -> Column:
    """
    Attempts to parse a column containing mixed date formats.
    Uses try_to_date to safely handle invalid dates (returns NULL instead of error).

    Tries multiple formats sequentially and returns the first successfully
    parsed date using coalesce.
    """
    result = F.lit(None).cast("date")
    for fmt in reversed(_DATE_FORMATS):
        result = F.coalesce(
            F.try_to_date(F.trim(F.col(col_name)), fmt),
            result
        )
    return result

# COMMAND ----------

def parse_timestamp_col(col_name: str) -> Column:
    """
    Attempts to parse a column containing mixed timestamp/date formats.
    Uses try_to_timestamp to safely handle invalid timestamps.

    Tries multiple formats sequentially and returns the first successfully
    parsed timestamp using coalesce.
    """
    ts_formats = [
        "d/M/yyyy HH:mm:ss",
        "d/M/yyyy",
        "dd/MM/yyyy HH:mm:ss",
        "dd/MM/yyyy",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd",
        "yyyy/MM/dd",
        "yyyy.MM.dd",
    ]
    result = F.lit(None).cast("timestamp")
    for fmt in reversed(ts_formats):
        result = F.coalesce(
            F.try_to_timestamp(F.trim(F.col(col_name)), fmt),
            result
        )
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phone Number Cleaning Function

# COMMAND ----------

def clean_phone_col(col_name: str) -> Column:
    """
    Cleans phone numbers by removing extensions, non-digits, and country codes.

    Transformations:
    1. Remove extensions (x314, ext123, #456)
    2. Remove all non-digit characters
    3. Remove leading country code (001, 1)
    4. Keep only valid 10-digit phone numbers

    Invalid or non-matching values safely return NULL.

    Examples:
        001-542-415-0246x314  -> 5424150246
        (674)865-6806x731     -> 6748656806
        718.871.8092x916      -> 7188718092
        7185624866            -> 7185624866
        invalid               -> NULL
    """
    phone = F.col(col_name)

    # Step 1: Remove extensions (everything after x, ext, #)
    phone = F.regexp_replace(phone, r'(?i)(x|ext|#).*$', '')

    # Step 2: Remove all non-digit characters
    phone = F.regexp_replace(phone, r'[^\d]', '')

    # Step 3: Remove leading country code (001 or 1) if more than 10 digits
    phone = F.when(
        F.length(phone) > 10,
        F.regexp_replace(phone, r'^(001|1)', '')
    ).otherwise(phone)

    # Step 4: Keep only 10-digit phones, return NULL otherwise
    result = F.when(F.length(phone) == 10, phone).otherwise(F.lit(None))

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
