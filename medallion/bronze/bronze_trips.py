import dlt
from _config.settings import INGEST_RUN_ID, WATERMARK_DAYS
from _common.dlt_utils import sanitize_columns, add_ingest_metadata
from _common.expectations import HASH_COLS
from _common.paths import CATALOG, BRONZE_SCHEMA,TRIPS_PATH

dlt.create_streaming_table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    partition_cols=["_ingest_yyyymmdd"],
)

@dlt.append_flow(target=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips")
def bronze_trips_append():
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
          .load(TRIPS_PATH)
    )
    df = sanitize_columns(df)
    df = add_ingest_metadata(df, INGEST_RUN_ID, "trips", HASH_COLS["trips"])
    return df.withWatermark("_ingest_ts", f"{WATERMARK_DAYS} days").dropDuplicates(["_record_hash"])
