from pyspark.sql.types import *

CITY_CONTRACT = StructType([
    StructField("city_id", StringType(), False),
    StructField("city_name", StringType(), False),
    StructField("_ingest_ts", TimestampType(), False),
    StructField("_ingest_date", DateType(), False),
    StructField("_ingest_yyyymmdd", IntegerType(), False),
    StructField("_source_file", StringType(), True),
    StructField("_source_file_mod_ts", TimestampType(), True),
    StructField("_source_file_size", LongType(), True),
    StructField("_ingest_run_id", StringType(), True),
    StructField("_record_hash", StringType(), True),
])

TRIPS_CONTRACT = StructType([
    StructField("trip_id", StringType(), False),
    StructField("city_id", StringType(), False),
    StructField("business_date", DateType(), False),
    StructField("distance_travelled", DoubleType(), False),
    StructField("passenger_type", StringType(), False),
    StructField("fare_amount", DoubleType(), False),
    StructField("passenger_rating", IntegerType(), False),
    StructField("driver_rating", IntegerType(), False),

    StructField("_ingest_ts", TimestampType(), False),
    StructField("_ingest_date", DateType(), False),
    StructField("_ingest_yyyymmdd", IntegerType(), False),
    StructField("_source_file", StringType(), True),
    StructField("_source_file_mod_ts", TimestampType(), True),
    StructField("_source_file_size", LongType(), True),
    StructField("_ingest_run_id", StringType(), True),
    StructField("_record_hash", StringType(), True),

    StructField("_replay_id", StringType(), True),
    StructField("_backfill_id", StringType(), True),
    StructField("_backfill_reason", StringType(), True),
])
