import dlt
import pyspark.sql.functions as F
from _common.paths import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.type_drift_guardrails",
    table_properties={"quality": "silver", "layer": "silver"},
)
def type_drift_guardrails():
    drift = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.type_drift_log")
    return (
        drift.groupBy("dataset","ingest_run_id")
             .agg(F.sum("type_drift_rows").alias("type_drift_rows"))
             .withColumn("_checked_at", F.current_timestamp())
    )