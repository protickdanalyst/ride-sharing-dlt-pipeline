import dlt
import pyspark.sql.functions as F
from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_trips_unmatched_city",
    table_properties={"quality": "silver", "layer": "silver"},
)
def silver_trips_unmatched_city():
    trips = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_trips")
    city_current = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_city").filter(F.col("__END_AT").isNull()).select("city_id")
    return trips.join(city_current, on="city_id", how="left_anti")
