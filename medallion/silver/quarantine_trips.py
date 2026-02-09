import dlt

from _common.paths import CATALOG, SILVER_SCHEMA

QUARANTINE_TRIPS = f"{CATALOG}.{SILVER_SCHEMA}.quarantine_trips"

@dlt.table(
    name=QUARANTINE_TRIPS,
    table_properties={"quality": "quarantine", "layer": "silver"},
)
def quarantine_trips():
    return dlt.read_stream("silver_trips_invalid")

