import pyspark.sql.functions as F
from pyspark.sql.types import StructType

def _cast(c, typ: str):
    return F.to_date(c) if typ == "date" else c.cast(typ)

def _apply_transform(c, t):
    if not t:
        return c
    if t == "upper":
        return F.upper(c)
    if t == "lower":
        return F.lower(c)
    if t == "initcap":
        return F.initcap(c)
    return c

def apply_mapping(df, mapping: dict, contract: StructType):
    cols = set(df.columns)
    out = []
    for f in contract.fields:
        name = f.name
        spec = mapping.get(name)
        if spec and spec["src"] in cols:
            c = _cast(F.col(spec["src"]), spec["cast"])
            c = _apply_transform(c, spec.get("transform"))
            out.append(c.cast(f.dataType).alias(name))
        elif name in cols:
            out.append(F.col(name).cast(f.dataType).alias(name))
        else:
            out.append(F.lit(None).cast(f.dataType).alias(name))
    return df.select(*out)
