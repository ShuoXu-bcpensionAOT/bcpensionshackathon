"""Built-in cleanse functions. Each registers itself via @cleanse_fn — extend by adding a
sibling module with more @cleanse_fn functions; nothing here needs editing."""
from pyspark.sql import functions as F

from . import cleanse_fn, register_cleanse_function


@cleanse_fn("trim")
def _cf_trim(df, cols, p):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c).cast("string")))
    return df


@cleanse_fn("normalize_text")
def _cf_normalize_text(df, cols, p):
    case = p.get("case")
    for c in cols:
        if c not in df.columns:
            continue
        col = F.trim(F.col(c).cast("string"))
        if p.get("collapse_spaces", True):
            col = F.regexp_replace(col, r"\s+", " ")
        if case == "lower":
            col = F.lower(col)
        elif case == "upper":
            col = F.upper(col)
        elif case == "title":
            col = F.initcap(col)
        if p.get("empty_as_null", True):
            col = F.when(col == "", None).otherwise(col)
        df = df.withColumn(c, col)
    return df


@cleanse_fn("fill_nulls")
def _cf_fill_nulls(df, cols, p):
    default = p.get("default", p.get("value"))
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.coalesce(F.col(c), F.lit(default)))
    return df


@cleanse_fn("parse_datetime")
def _cf_parse_datetime(df, cols, p):
    conv = F.to_date if p.get("target_type", "date") == "date" else F.to_timestamp
    formats = p.get("formats", ["yyyy-MM-dd"])
    for c in cols:
        if c not in df.columns:
            continue
        parsed = F.lit(None)
        for fmt in formats:
            parsed = F.coalesce(parsed, conv(F.col(c).cast("string"), fmt))
        df = df.withColumn(p.get("into") or c, parsed)
    return df


def _cf_case(fn):
    def apply(df, cols, p):
        for c in cols:
            if c in df.columns:
                df = df.withColumn(c, fn(F.col(c).cast("string")))
        return df
    return apply


@cleanse_fn("replace")
def _cf_replace(df, cols, p):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.regexp_replace(F.col(c).cast("string"),
                                                   p.get("pattern", ""), p.get("replacement", "")))
    return df


@cleanse_fn("mask")
def _cf_mask(df, cols, p):
    """Static masking (stored masked — enforced on EVERY engine). style: redact|hash|partial."""
    style = p.get("style", "redact")
    for c in cols:
        if c not in df.columns:
            continue
        col = F.col(c).cast("string")
        if style == "hash":
            df = df.withColumn(c, F.sha2(col, 256))
        elif style == "partial":                        # keep last N chars, e.g. ***1234
            keep = int(p.get("keep", 4))
            df = df.withColumn(c, F.concat(F.lit(p.get("prefix", "***")),
                                           F.substring(col, -keep, keep)))
        else:                                            # redact
            df = df.withColumn(c, F.lit(p.get("replacement", "***")))
    return df


register_cleanse_function("to_upper", _cf_case(F.upper))
register_cleanse_function("to_lower", _cf_case(F.lower))
register_cleanse_function("to_title", _cf_case(F.initcap))
