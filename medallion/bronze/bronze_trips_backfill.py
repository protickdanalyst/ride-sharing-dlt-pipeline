import dlt
from _config.settings import INGEST_RUN_ID, WATERMARK_DAYS, BACKFILL_MODE
from _common.dlt_utils import sanitize_columns, add_ingest_metadata
from _common.expectations import HASH_COLS
from _common.paths import CATALOG, BRONZE_SCHEMA,TRIPS_BACKFILL_PATH

dlt.create_streaming_table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips_backfill",
    table_properties={"quality": "bronze", "layer": "bronze"},
    partition_cols=["_ingest_yyyymmdd"],
)

@dlt.append_flow(target=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips_backfill")
def bronze_trips_backfill_append():
    if not BACKFILL_MODE:
        return dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips").where("1=0")

    df = (
        spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.inferColumnTypes", "false")
          .option("cloudFiles.schemaEvolutionMode", "rescue")
          .option("rescuedDataColumn", "_rescued_data")
          .option("cloudFiles.includeExistingFiles", "true")
          .option("cloudFiles.validateOptions", "true")
          .option("cloudFiles.maxFilesPerTrigger", "1000")
          .option("header", "true")
          .option("multiLine", "true")
          .option("quote", '"')
          .option("escape", '"')
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .load(TRIPS_BACKFILL_PATH)
    )
    df = sanitize_columns(df)
    df = add_ingest_metadata(df, INGEST_RUN_ID, "trips_backfill", HASH_COLS["trips"])
    return df.withWatermark("_ingest_ts", f"{WATERMARK_DAYS} days").dropDuplicates(["_record_hash"])
