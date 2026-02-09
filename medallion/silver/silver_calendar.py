import dlt
import pyspark.sql.functions as F
from _common.paths import CATALOG, SILVER_SCHEMA

SILVER_CALENDAR = f"{CATALOG}.{SILVER_SCHEMA}.silver_calendar"

@dlt.table(
    name=SILVER_CALENDAR,
    table_properties={"quality": "silver", "layer": "silver"},
)
def silver_calendar():
    df = spark.createDataFrame([("2020-01-01", "2030-12-31")], ["start", "end"])
    return (
        df.select(F.explode(F.sequence(F.to_date("start"), F.to_date("end"))).alias("date"))
          .select(
              F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
              F.col("date").cast("date").alias("date"),
              F.year("date").cast("int").alias("year"),
              F.month("date").cast("int").alias("month"),
              F.dayofmonth("date").cast("int").alias("day"),
          )
    )
