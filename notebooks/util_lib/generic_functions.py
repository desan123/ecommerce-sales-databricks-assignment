from pyspark.sql import functions as F
from delta.tables import DeltaTable


def upsert_logic_bronze(
    source_df,
    table_name: str,
    merge_keys: list,
    schema,
    partition_cols: list = None,
):
    """
    Writes source_df to a Delta table at table_name using MERGE on merge_keys.
    Creates the table on first run; upserts on subsequent runs.
    """
    # Ensure schema columns exist (add nulls for any missing optional cols)
    for field in schema.fields:
        if field.name not in source_df.columns:
            source_df = source_df.withColumn(field.name, F.lit(None).cast(field.dataType))

    source_df = source_df.select([f.name for f in schema.fields])

    if not spark.catalog.tableExists(table_name):
        print(f"Table {table_name} does not exist — creating on first run ...")
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(table_name)
        return

    delta_table = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    print(f"Upserting {table_name} ...")
    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def upsert_logic(
    source_df,
    table_name: str,
    merge_keys: list,
    partition_cols: list = None,
):
    """
    Upserts source_df into an existing Delta table using MERGE on merge_keys.
    Creates the table on first run if it does not already exist.
    """
    if not spark.catalog.tableExists(table_name):
        print(f"Table {table_name} does not exist — creating on first run ...")
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(table_name)
        return

    delta_table = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    print(f"Upserting {table_name} ...")
    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
