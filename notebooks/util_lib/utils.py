import unicodedata
import re
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql import Column
from pyspark.sql.types import StringType as _ST

# ---------------------------------------------------------------------------
# Date parsing
# Handles formats seen in the dataset: YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD
# and the Timestamp variant YYYY-MM-DDTHH:MM:SS
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
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


def parse_date_col(col_name: str) -> Column:
    """
    Attempts to parse a column containing mixed timestamp/date formats.

    The function tries multiple formats sequentially and returns the first
    successfully parsed timestamp using coalesce. Invalid or non-matching
    values safely return NULL due to try_to_date().
    """
    result = F.lit(None).cast("date")
    for fmt in reversed(_DATE_FORMATS):
        result = F.coalesce(
            F.try_to_date(F.trim(F.col(col_name)), F.lit(fmt)),
            result
        )
    return result


def parse_timestamp_col(col_name: str) -> Column:
    """
    Attempts to parse a column containing mixed timestamp/date formats.

    The function tries multiple formats sequentially and returns the first
    successfully parsed timestamp using coalesce. Invalid or non-matching
    values safely return NULL due to try_to_timestamp().
    """
    ts_formats = [
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd",
        "yyyy/MM/dd",
        "yyyy.MM.dd",
    ]
    result = F.lit(None).cast("timestamp")
    for fmt in reversed(ts_formats):
        result = F.coalesce(
            F.try_to_timestamp(F.trim(F.col(col_name)), F.lit(fmt)),
            result
        )
    return result

# ---------------------------------------------------------------------------
# Unicode normalisation
# ---------------------------------------------------------------------------

def normalize_unicode(text: Optional[str]) -> Optional[str]:
    """
    This function normalizes Unicode text using NFC(Normalization Form Canonical Composition) normalization to ensure consistent representation of characters across the dataset.
    """
    if text is None:
        return None
    return unicodedata.normalize("NFC", text)

def get_normalize_udf():
    from pyspark.sql.functions import udf
    return udf(normalize_unicode, _ST())
