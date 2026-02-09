import dlt
import pyspark.sql.functions as F
from _common.rescued_utils import rescued_as_map

from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.rescued_trips_profile",
    table_properties={"quality":"silver","layer":"silver"},
)
def rescued_trips_profile():
    df = rescued_as_map(dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips"))
    return (
        df.select(F.explode(F.map_keys(F.col("_rescued_map"))).alias("rescued_key"))
          .groupBy("rescued_key")
          .count()
          .orderBy(F.desc("count"))
    )
