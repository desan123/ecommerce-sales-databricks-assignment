# Databricks notebook source

# MAGIC %md
# MAGIC # Generic Functions
# MAGIC
# MAGIC Reusable utility functions for Delta Lake upsert operations.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Functions

# COMMAND ----------

def upsert_logic_bronze(
    source_df,
    table_name: str,
    merge_keys: list,
    schema,
    partition_cols: list = None,
):
    """
    Writes source_df to a Delta table at table_name using MERGE on merge_keys.
    Creates the table on first run with column mapping enabled for spaces in column names.

    Args:
        source_df: Source DataFrame to upsert
        table_name: Full table name (database.table)
        merge_keys: List of column names to use as merge keys
        schema: Expected schema for the table
        partition_cols: Optional list of partition columns
    """
    # Ensure schema columns exist (add nulls for any missing optional cols)
    for field in schema.fields:
        if field.name not in source_df.columns:
            source_df = source_df.withColumn(field.name, F.lit(None).cast(field.dataType))

    # Select in schema order and cast to the declared types (e.g. Excel numeric
    # Postal Code -> string) so the table always matches the schema.
    source_df = source_df.select(
        [F.col(f"`{f.name}`").cast(f.dataType).alias(f.name) for f in schema.fields]
    )

    if not spark.catalog.tableExists(table_name):
        print(f"Table {table_name} does not exist — creating with column mapping enabled...")
        writer = (source_df.write
                  .format("delta")
                  .mode("overwrite")
                  .option("delta.columnMapping.mode", "name")
                  .option("delta.minReaderVersion", "2")
                  .option("delta.minWriterVersion", "5"))

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.saveAsTable(table_name)
        print(f"Table {table_name} created with column mapping enabled")
        return

    delta_table = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in merge_keys])

    print(f"Upserting {table_name}...")
    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Upsert complete")

# COMMAND ----------

def upsert_logic(
    source_df,
    table_name: str,
    merge_keys: list,
    partition_cols: list = None,
):
    """
    Upserts source_df into an existing Delta table using MERGE on merge_keys.
    Creates the table on first run if it does not already exist.

    Args:
        source_df: Source DataFrame to upsert
        table_name: Full table name (database.table)
        merge_keys: List of column names to use as merge keys
        partition_cols: Optional list of partition columns
    """
    if not spark.catalog.tableExists(table_name):
        print(f"Table {table_name} does not exist — creating on first run...")
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(table_name)
        print(f"Table {table_name} created")
        return

    delta_table = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    print(f"Upserting {table_name}...")
    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Upsert complete")

# COMMAND ----------

def overwrite_logic(df, table_name: str):
    """
    Full-refresh write of a derived (gold) table.

    Gold tables are rebuilt entirely from Silver, so overwrite is idempotent and
    avoids MERGE problems with NULL keys (e.g. orders whose product is missing
    have a NULL category, and NULL = NULL never matches in a merge condition).
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"{table_name} overwritten")
