import dlt
import pyspark.sql.functions as F

from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_trips_asof",
    table_properties={"quality": "gold", "layer": "gold"},
)
def fact_trips_asof():
    trips = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_trips")
    city_scd2 = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_city")
    date = dlt.read(f"{CATALOG}.{GOLD_SCHEMA}.dim_date")

    bd_ts = F.to_timestamp(F.col("business_date"))

    city_valid = (
        city_scd2
        .withColumn("__END_AT_COALESCE", F.coalesce(F.col("__END_AT"), F.to_timestamp(F.lit("2999-12-31"))))
    )

    joined = trips.join(
        city_valid,
        on=[
            trips.city_id == city_valid.city_id,
            bd_ts >= city_valid["__START_AT"],
            bd_ts < city_valid["__END_AT_COALESCE"],
        ],
        how="inner",
    )

    return (
        joined.join(date, joined.business_date == date.date, "inner")
              .select(
                  joined.trip_id.alias("trip_id"),
                  joined.city_id.alias("city_id"),
                  joined.city_name.alias("city_name_asof"),
                  F.col("date_key").cast("int").alias("date_key"),
                  F.col("year").cast("int").alias("year"),
                  F.col("month").cast("int").alias("month"),
                  F.lit(1).cast("int").alias("trip_count"),
                  joined.distance_travelled.alias("distance_travelled"),
                  joined.fare_amount.alias("fare_amount"),
                  joined.passenger_rating.alias("passenger_rating"),
                  joined.driver_rating.alias("driver_rating"),
                  joined.passenger_type.alias("passenger_type"),
                  F.current_timestamp().alias("_load_ts"),
                  joined._ingest_run_id.alias("_ingest_run_id"),
                  joined._replay_id.alias("_replay_id"),
                  joined._backfill_id.alias("_backfill_id"),
              )
    )
