import dlt
from _common.paths import CATALOG, SILVER_SCHEMA

QUARANTINE_CITY = f"{CATALOG}.{SILVER_SCHEMA}.quarantine_city"

@dlt.table(
    name=QUARANTINE_CITY,
    table_properties={"quality": "quarantine", "layer": "silver"},
)
def quarantine_city():
    return dlt.read_stream("silver_city_invalid")