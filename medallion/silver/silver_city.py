import dlt
import pyspark.sql.functions as F

from _common.contracts import CITY_CONTRACT
from _common.mappings import CITY_MAPPING
from _common.transformers import apply_mapping
from _common.rescued_utils import rescued_as_map, promote_from_rescued, drop_rescued_map
from _config.rescued_field_registry import RESCUED_PROMOTION

from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

BRONZE_CITY = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_city"
SILVER_CITY = f"{CATALOG}.{SILVER_SCHEMA}.silver_city"

@dlt.view
def silver_city_staging():
    df = dlt.read_stream(BRONZE_CITY)
    df = rescued_as_map(df)
    df = promote_from_rescued(df, RESCUED_PROMOTION.get("city", {}))
    df = drop_rescued_map(df)
    df = apply_mapping(df, CITY_MAPPING, CITY_CONTRACT)
    return df.select(*CITY_CONTRACT.fieldNames())

@dlt.view
@dlt.expect_or_drop("city_id_nn", "city_id IS NOT NULL")
@dlt.expect_or_drop("city_name_nn", "city_name IS NOT NULL")
def silver_city_valid():
    return dlt.read_stream("silver_city_staging")

@dlt.view
def silver_city_invalid():
    s = dlt.read_stream("silver_city_staging")
    return (
        s.filter("city_id IS NULL OR city_name IS NULL")
         .withColumn(
             "_reject_reason",
             F.when(F.col("city_id").isNull(), F.lit("missing_city_id"))
              .when(F.col("city_name").isNull(), F.lit("missing_city_name"))
              .otherwise(F.lit("unknown"))
         )
    )

dlt.create_streaming_table(
    name=SILVER_CITY,
    table_properties={"quality": "silver", "layer": "silver"},
)

dlt.apply_changes(
    target=SILVER_CITY,
    source="silver_city_valid",
    keys=["city_id"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)