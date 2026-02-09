import dlt
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from _config.settings import INGEST_RUN_ID
from _common.expectations import CITY_REQUIRED_COLS, TRIPS_REQUIRED_COLS
from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

SCHEMA_DRIFT_SCHEMA = StructType([
    StructField("dataset", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("ingest_run_id", StringType(), False),
    StructField("actual_cols", ArrayType(StringType(), False), False),
    StructField("required_cols", ArrayType(StringType(), False), False),
    StructField("missing_required_cols", ArrayType(StringType(), False), False),
    StructField("is_breaking_drift", BooleanType(), False),
])

def _evt(dataset: str, layer: str, required_cols: list[str], df, run_id: str):
    actual = df.schema.fieldNames()
    missing = [c for c in required_cols if c not in actual]
    return spark.createDataFrame(
        [Row(
            dataset=dataset,
            layer=layer,
            ingest_run_id=run_id,
            actual_cols=actual,
            required_cols=required_cols,
            missing_required_cols=missing,
            is_breaking_drift=len(missing) > 0,
        )],
        schema=SCHEMA_DRIFT_SCHEMA,
    )

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.schema_drift_log",
    table_properties={"quality": "silver", "layer": "silver"},
)
def schema_drift_log():
    city = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_city")
    trips = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips")
    return _evt("city", "bronze", CITY_REQUIRED_COLS, city, INGEST_RUN_ID).unionByName(
        _evt("trips", "bronze", TRIPS_REQUIRED_COLS, trips, INGEST_RUN_ID)
    )
