import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType

MAP_SS = MapType(StringType(), StringType())

def _empty_map_ss():
    return F.from_json(F.lit("{}"), MAP_SS)

def rescued_as_map(df, out_col: str = "_rescued_map"):
    if out_col in df.columns:
        return df.withColumn(out_col, F.col(out_col).cast(MAP_SS))
    if "_rescued_data" not in df.columns:
        return df.withColumn(out_col, _empty_map_ss())
    m = F.from_json(F.to_json(F.col("_rescued_data")), MAP_SS)
    return df.withColumn(out_col, F.coalesce(m, _empty_map_ss()).cast(MAP_SS))

def promote_from_rescued(df, promotion: dict, rescued_map_col: str = "_rescued_map"):
    if not promotion:
        return df
    df = rescued_as_map(df, rescued_map_col)
    for field, typ in promotion.items():
        rv = F.col(rescued_map_col).getItem(field).cast(typ)
        df = df.withColumn(field, F.coalesce(F.col(field).cast(typ), rv) if field in df.columns else rv)
        df = df.withColumn(f"_{field}_from_rescued", rv.isNotNull())
    return df

def drop_rescued_map(df, rescued_map_col: str = "_rescued_map"):
    return df.drop(rescued_map_col) if rescued_map_col in df.columns else df
