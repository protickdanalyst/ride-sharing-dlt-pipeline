import dlt
import pyspark.sql.functions as F
from functools import reduce

from _config.settings import INGEST_RUN_ID
from _common.expectations import TYPE_DRIFT_FIELDS
from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

def _cast_expr(c, typ: str):
    return F.to_date(c) if typ == "date" else c.cast(typ)

def _drift_rows(df, dataset: str, fields: dict):
    cols = set(df.columns)
    selects = []
    for field, typ in fields.items():
        if field not in cols:
            continue
        raw = F.col(field)
        casted = _cast_expr(raw, typ)
        selects.append(
            df.select(
                F.lit(dataset).alias("dataset"),
                F.lit(field).alias("field"),
                F.lit(typ).alias("expected_type"),
                F.lit(INGEST_RUN_ID).alias("ingest_run_id"),
                F.when(raw.isNotNull() & casted.isNull(), F.lit(1)).otherwise(F.lit(0)).alias("is_type_drift"),
            )
        )
    return selects

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.type_drift_log",
    table_properties={"quality": "silver", "layer": "silver"},
)
def type_drift_log():
    trips = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_trips")
    city = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_city")

    selects = []
    selects += _drift_rows(trips, "trips", TYPE_DRIFT_FIELDS.get("trips", {}))
    selects += _drift_rows(city, "city", TYPE_DRIFT_FIELDS.get("city", {}))

    if not selects:
        return spark.createDataFrame(
            [],
            "dataset STRING, field STRING, expected_type STRING, ingest_run_id STRING, type_drift_rows BIGINT, _logged_at TIMESTAMP",
        )

    unioned = reduce(lambda a, b: a.unionByName(b), selects)

    return (
        unioned.groupBy("dataset", "field", "expected_type", "ingest_run_id")
              .agg(F.sum("is_type_drift").cast("bigint").alias("type_drift_rows"))
              .withColumn("_logged_at", F.current_timestamp())
    )
