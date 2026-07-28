"""Ad-hoc file connector for the dropbox intake. Reads ONE object — a csv/txt file, or ONE sheet
of an xlsx/xls workbook — from a OneLake Files path into a **string-typed** Spark DataFrame so the
silver full-row-hash stays stable across re-drops. source_options_json:
    file_abfss   full abfss path to the file (LH_Filedrop/Files/...)
    format       csv | txt | xlsx | xls
    sheet        sheet name (xlsx/xls only)
    sep          delimiter override (csv/txt); omit to default ',' for csv and sniff for txt
"""
import os
import re
import tempfile

from ..runtime import spark, notebookutils
from ..naming import _norm_ident, snake, landed_table, unique_names
from ..audit import record_column_map
from . import ingest_connector
from .base import _opts, _ensure_pkg


def _local_copy(abfss):
    local = os.path.join(tempfile.gettempdir(),
                         f"cp_drop_{_norm_ident(abfss.rsplit('/', 1)[-1])}_{abs(hash(abfss)) % 10**8}")
    notebookutils.fs.cp(abfss, "file:" + local, True)
    return local


@ingest_connector("file", "dropbox")
def file_source(o, user, password):
    import pandas as pd
    opts = _opts(o)
    fmt = (opts.get("format") or "").lower()
    local = _local_copy(opts["file_abfss"])
    try:
        if fmt in ("xlsx", "xls"):
            engine = "openpyxl" if fmt == "xlsx" else "xlrd"
            _ensure_pkg(engine)
            pdf = pd.read_excel(local, sheet_name=opts["sheet"], dtype=str, engine=engine)
        else:                                                # csv / txt
            sep = opts.get("sep")
            if sep is None and fmt == "txt":                 # sniff the delimiter for txt
                pdf = pd.read_csv(local, dtype=str, sep=None, engine="python")
            else:
                pdf = pd.read_csv(local, dtype=str, sep=sep or ",", low_memory=False)
    finally:
        try:
            os.remove(local)
        except OSError:
            pass

    pdf = pdf.dropna(axis=1, how="all")                      # drop fully-empty columns
    originals = [str(c) for c in pdf.columns]                # true headers (may contain ':' '-' etc.)
    raw = [re.sub(r"[^A-Za-z0-9_]", "_", c).strip("_") or f"col{i}" for i, c in enumerate(originals)]
    sanitized = unique_names(raw)                            # unique bronze names (no duplicate cols)
    physical = unique_names([snake(s) for s in sanitized])   # final silver names, unique + stable
    pdf.columns = sanitized
    sch, tbl = landed_table(o)
    if len(set(raw)) != len(raw) or len(set(snake(s) for s in sanitized)) != len(sanitized):
        print(f"file connector [{sch}.{tbl}]: header collision de-duplicated "
              f"(physical names suffixed _2/_3; true originals preserved in column_map)")
    # Record the original header for every column whose LANDED (silver) physical name differs, so a
    # later unpivot can restore the real business value (incl. ':' '-'). Only changed columns stored.
    record_column_map(o.get("object_id"), sch, tbl, list(zip(physical, originals)))
    pdf = pdf.where(pd.notnull(pdf), None)
    if pdf.empty:
        schema = ", ".join(f"`{c}` string" for c in pdf.columns) or "_empty string"
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(pdf)
