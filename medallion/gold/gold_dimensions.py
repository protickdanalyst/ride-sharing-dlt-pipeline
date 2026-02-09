import dlt
import pyspark.sql.functions as F

from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.table(name=f"{CATALOG}.{GOLD_SCHEMA}.dim_city_current", table_properties={"quality": "gold", "layer": "gold"})
def dim_city_current():
    return dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_city").filter(F.col("__END_AT").isNull()).select("city_id", "city_name")

@dlt.table(name=f"{CATALOG}.{GOLD_SCHEMA}.dim_date", table_properties={"quality": "gold", "layer": "gold"})
def dim_date():
    return dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_calendar")
