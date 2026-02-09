import dlt
import pyspark.sql.functions as F

from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_trips_current",
    table_properties={"quality": "gold", "layer": "gold"},
)
def fact_trips_current():
    trips = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_trips")
    city = dlt.read(f"{CATALOG}.{GOLD_SCHEMA}.dim_city_current")
    date = dlt.read(f"{CATALOG}.{GOLD_SCHEMA}.dim_date")

    return (
        trips.join(city, "city_id", "inner")
             .join(date, trips.business_date == date.date, "inner")
             .select(
                 "trip_id",
                 "city_id",
                 "date_key",
                 "year",
                 "month",
                 F.lit(1).cast("int").alias("trip_count"),
                 "distance_travelled",
                 "fare_amount",
                 "passenger_rating",
                 "driver_rating",
                 "passenger_type",
                 F.current_timestamp().alias("_load_ts"),
                 "_ingest_run_id",
                 "_replay_id",
                 "_backfill_id",
             )
    )
