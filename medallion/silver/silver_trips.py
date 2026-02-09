import dlt
import pyspark.sql.functions as F

from _common.contracts import TRIPS_CONTRACT
from _common.mappings import TRIPS_MAPPING
from _common.transformers import apply_mapping
from _common.rescued_utils import rescued_as_map, promote_from_rescued, drop_rescued_map
from _config.rescued_field_registry import RESCUED_PROMOTION
from _config.settings import (
    LATE_WINDOW_DAYS, WATERMARK_DAYS,
    ENABLE_LATE_REPLAY, REPLAY_ID,
    BACKFILL_MODE, BACKFILL_START_DATE, BACKFILL_END_DATE, BACKFILL_ID, BACKFILL_REASON
)

from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

def _late_cutoff(df):
    return F.date_sub(F.col("_ingest_date"), LATE_WINDOW_DAYS)

def _empty_stream_like(ref_stream_df):
    cols = [F.lit(None).cast(f.dataType).alias(f.name) for f in TRIPS_CONTRACT.fields]
    return ref_stream_df.select(*cols).where("1=0")

@dlt.view
def silver_trips_staging():
    df = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips")
    df = rescued_as_map(df)
    df = promote_from_rescued(df, RESCUED_PROMOTION.get("trips", {}))
    df = drop_rescued_map(df)
    df = apply_mapping(df, TRIPS_MAPPING, TRIPS_CONTRACT)
    return df.withColumn("_is_late_arrival", F.col("business_date") < _late_cutoff(df))

@dlt.view
@dlt.expect_or_drop("trip_id_nn", "trip_id IS NOT NULL")
@dlt.expect_or_drop("city_id_nn", "city_id IS NOT NULL")
@dlt.expect_or_drop("business_date_nn", "business_date IS NOT NULL")
@dlt.expect_or_drop("distance_ok", "distance_travelled >= 0")
@dlt.expect_or_drop("fare_ok", "fare_amount >= 0")
@dlt.expect_or_drop("passenger_rating_ok", "passenger_rating BETWEEN 1 AND 5")
@dlt.expect_or_drop("driver_rating_ok", "driver_rating BETWEEN 1 AND 5")
def silver_trips_valid():
    df = (
        dlt.read_stream("silver_trips_staging")
        .withWatermark("_ingest_ts", f"{WATERMARK_DAYS} days")
        .filter("_is_late_arrival = false")
        .drop("_is_late_arrival")
    )
    return df.select(*TRIPS_CONTRACT.fieldNames())

@dlt.view
def silver_trips_invalid():
    s = dlt.read_stream("silver_trips_staging")
    return (
        s.filter("""
            trip_id IS NULL OR
            city_id IS NULL OR
            business_date IS NULL OR
            distance_travelled < 0 OR
            fare_amount < 0 OR
            passenger_rating < 1 OR passenger_rating > 5 OR
            driver_rating < 1 OR driver_rating > 5 OR
            _is_late_arrival = true
        """)
        .withColumn(
            "_reject_reason",
            F.when(F.col("trip_id").isNull(), F.lit("missing_trip_id"))
             .when(F.col("city_id").isNull(), F.lit("missing_city_id"))
             .when(F.col("business_date").isNull(), F.lit("missing_business_date"))
             .when(F.col("distance_travelled") < 0, F.lit("negative_distance"))
             .when(F.col("fare_amount") < 0, F.lit("negative_fare"))
             .when((F.col("passenger_rating") < 1) | (F.col("passenger_rating") > 5), F.lit("passenger_rating_out_of_range"))
             .when((F.col("driver_rating") < 1) | (F.col("driver_rating") > 5), F.lit("driver_rating_out_of_range"))
             .when(F.col("_is_late_arrival") == True, F.lit("late_arrival"))
             .otherwise(F.lit("unknown"))
        )
    )

@dlt.view
def silver_trips_replay_late_only():
    ref = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips")
    if not ENABLE_LATE_REPLAY:
        return _empty_stream_like(ref)
    if REPLAY_ID is None:
        raise Exception("enable_late_replay=true requires replay_id")

    return (
        dlt.read_stream(f"{CATALOG}.{SILVER_SCHEMA}.quarantine_trips")
        .filter("_reject_reason = 'late_arrival'")
        .withColumn("_replay_id", F.lit(REPLAY_ID))
        .select(*TRIPS_CONTRACT.fieldNames())
    )

@dlt.view
def silver_trips_backfill_source():
    ref = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips")
    if not BACKFILL_MODE:
        return _empty_stream_like(ref)
    if BACKFILL_ID is None or BACKFILL_REASON is None:
        raise Exception("backfill_mode=true requires backfill_id and backfill_reason")

    df = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips_backfill")
    df = rescued_as_map(df)
    df = promote_from_rescued(df, RESCUED_PROMOTION.get("trips", {}))
    df = drop_rescued_map(df)
    df = apply_mapping(df, TRIPS_MAPPING, TRIPS_CONTRACT)

    df = df.filter(
        (F.col("business_date") >= F.to_date(F.lit(BACKFILL_START_DATE))) &
        (F.col("business_date") <= F.to_date(F.lit(BACKFILL_END_DATE)))
    )

    df = df.withColumn("_backfill_id", F.lit(BACKFILL_ID)).withColumn("_backfill_reason", F.lit(BACKFILL_REASON))
    return df.select(*TRIPS_CONTRACT.fieldNames())

@dlt.view
def silver_trips_cdc_source():
    base = dlt.read_stream("silver_trips_valid")
    replay = dlt.read_stream("silver_trips_replay_late_only")
    backfill = dlt.read_stream("silver_trips_backfill_source")
    return base.unionByName(replay).unionByName(backfill)

dlt.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_trips",
    schema=TRIPS_CONTRACT,
    table_properties={"quality": "silver", "layer": "silver"},
)

dlt.apply_changes(
    target=f"{CATALOG}.{SILVER_SCHEMA}.silver_trips",
    source="silver_trips_cdc_source",
    keys=["trip_id"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=1,
)
