import re
import pyspark.sql.functions as F

def sanitize_columns(df):
    for c in df.columns:
        nc = c.strip().lower()
        nc = re.sub(r"[^\w]+", "_", nc)
        nc = re.sub(r"_+", "_", nc).strip("_")
        if c != nc:
            df = df.withColumnRenamed(c, nc)
    return df

def _norm_str(c, upper=False):
    s = F.trim(F.regexp_replace(F.coalesce(c.cast("string"), F.lit("")), r"\s+", " "))
    return F.upper(s) if upper else s

def _norm_double(c):
    return F.format_number(c.cast("double"), 6)

def _norm_date(c):
    return F.date_format(F.to_date(c), "yyyy-MM-dd")

def add_ingest_metadata(df, run_id: str, dataset: str, hash_cols: list[str]):
    cols = set(df.columns)
    ingest_ts = F.current_timestamp()
    ingest_date = F.to_date(ingest_ts)

    norm_exprs = []
    for c in hash_cols:
        if c not in cols:
            norm_exprs.append(F.lit(""))
            continue
        if c == "city_id":
            norm_exprs.append(_norm_str(F.col(c), upper=True))
        elif c == "trip_id":
            norm_exprs.append(_norm_str(F.col(c)))
        elif c == "date":
            norm_exprs.append(_norm_date(F.col(c)))
        elif c == "distance_travelled_km":
            norm_exprs.append(_norm_double(F.col(c)))
        else:
            norm_exprs.append(_norm_str(F.col(c)))

    return (
        df.withColumn("_ingest_ts", ingest_ts)
          .withColumn("_ingest_date", ingest_date)
          .withColumn("_ingest_yyyymmdd", F.date_format(ingest_date, "yyyyMMdd").cast("int"))
          .withColumn("_dataset", F.lit(dataset))
          .withColumn("_ingest_run_id", F.lit(run_id))
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_source_file_mod_ts", F.col("_metadata.file_modification_time").cast("timestamp"))
          .withColumn("_source_file_size", F.col("_metadata.file_size").cast("long"))
          .withColumn("_record_hash", F.sha2(F.concat_ws("||", *norm_exprs), 256))
    )
