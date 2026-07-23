"""Dropbox intake entrypoint — process ONE dropped file (event-triggered, one run per file).

Landing lakehouse LH_Filedrop/Files:
    newfile/<schema>/<file>   -> medallion schema = <schema>
    newfile/<file>            -> medallion schema = dbo
Each csv/txt file -> one table; each xlsx/xls sheet -> its own table (<file>_<tab>).
Bronze APPENDS every drop; silver dedups on the full row hash. Idempotent via the
dropbox_ledger (skip re-drops of identical content) AND the silver row-hash dedup (so even a
duplicate event can't create duplicate rows). Processed files are moved to archive/Y/M/D/<ts>/.
"""
import json
import os
import tempfile
import traceback

from pyspark.sql import functions as F

from ..runtime import WS_ID, notebookutils, tpath
from ..naming import _norm_ident, now_ts
from ..storage import delta_exists, read_path, files_put
from ..config_db import config_query, config_exec
from ..audit import append_rows, seed_control_tables
from ..connectors.base import _ensure_pkg
from .bronze import bronze
from .silver import silver

FILEDROP_LAKEHOUSE = "LH_Filedrop"


def _filedrop_guid():
    for lh in notebookutils.lakehouse.list():
        if lh["displayName"] == FILEDROP_LAKEHOUSE:
            return lh["id"]
    raise Exception(f"lakehouse {FILEDROP_LAKEHOUSE} not found")


def _download(abfss, base, tries=3):
    """Copy the OneLake file locally; retry (Excel/clients can fire the event mid-write)."""
    import time
    last = None
    for _ in range(tries):
        try:
            local = os.path.join(tempfile.gettempdir(), f"cp_dropin_{base}_{abs(hash(abfss)) % 10**8}")
            notebookutils.fs.cp(abfss, "file:" + local, True)
            return local
        except Exception as e:
            last = e
            time.sleep(3)
    raise last


def _hash_file(local):
    import hashlib
    h = hashlib.sha256()
    with open(local, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _rm(local):
    try:
        os.remove(local)
    except OSError:
        pass


def _already_processed(file_key, fhash):
    p = tpath("config", "dropbox_ledger")
    if not delta_exists(p):
        return False
    return read_path(p).where((F.col("file_key") == file_key) &
                              (F.col("content_hash") == fhash)).limit(1).count() > 0


def _ensure_datasource(schema):
    """Idempotently register the schema as a `file` datasource; return its source_id."""
    config_exec(
        "IF NOT EXISTS (SELECT 1 FROM dbo.datasource WHERE source_name=?) "
        "INSERT INTO dbo.datasource (source_name, source_type, load_group, ingestion_mode, "
        "is_active, connector) VALUES (?, 'FILE', 0, 'file', 1, 'file')", (schema, schema))
    return config_query("SELECT MIN(source_id) AS sid FROM dbo.datasource WHERE source_name=?",
                        (schema,))[0]["sid"]


def _register_object(oid, sid, tbl, keys_json, opts_json):
    config_exec("DELETE FROM dbo.source_object WHERE object_id=?", (oid,))
    config_exec(
        "INSERT INTO dbo.source_object (object_id, source_id, source_table, target_name, load_type,"
        " key_columns_json, is_active, processing_state, source_options_json) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (oid, sid, tbl, f"dropbox_{tbl}", "append", keys_json, 1, "ACTIVE", opts_json))


def _write_ledger(file_key, fhash, schema, n):
    append_rows("dropbox_ledger", [{"file_key": file_key, "content_hash": fhash,
                                    "schema_name": schema, "object_count": n,
                                    "status": "PROCESSED", "processed_at": now_ts()}])


def _archive(guid, rel):
    # Archive by DATE (not per-run timestamp) so a batch dropped together lands in ONE day folder,
    # even though each file is processed by its own event-triggered run. Concurrent runs just mv
    # into the same folder (distinct filenames). Same-name re-drops in a day get a time suffix.
    ts = now_ts()
    fname = rel.rsplit("/", 1)[-1]
    root = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{guid}/Files"
    folder = f"{root}/archive/{ts:%Y/%m/%d}"
    src = f"{root}/{rel}"
    try:
        notebookutils.fs.mv(src, f"{folder}/{fname}", True)
        print(f"dropbox: archived -> archive/{ts:%Y/%m/%d}/{fname}")
    except Exception:                                        # name already there (re-drop) — keep both
        stem, dot, ext = fname.rpartition(".")
        alt = f"{(stem or fname)}_{ts:%H%M%S}{('.' + ext) if dot else ''}"
        try:
            notebookutils.fs.mv(src, f"{folder}/{alt}", True)
            print(f"dropbox: archived -> archive/{ts:%Y/%m/%d}/{alt}")
        except Exception as e:
            print("dropbox: archive mv warning:", str(e)[:150])


def _run(file_path, run_id):
    seed_control_tables()
    guid = _filedrop_guid()
    # the OneLake event Subject is URL-encoded (spaces -> %20) and prefixed (/Files/newfile/..);
    # decode, then normalize to start at 'newfile' regardless of any prefix.
    import urllib.parse
    file_path = urllib.parse.unquote(file_path or "")
    parts = file_path.strip("/").replace("\\", "/").split("/")
    if "newfile" in parts:
        parts = parts[parts.index("newfile"):]
    if not parts or parts[0] != "newfile" or len(parts) < 2:
        print(f"dropbox: '{file_path}' is not a file under newfile/ — ignored")
        return "ignored (not under newfile/)"
    rel = "/".join(parts)
    fname = parts[-1]
    schema = _norm_ident(parts[1]) if len(parts) >= 3 else "dbo"
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
    base = _norm_ident(fname.rsplit(".", 1)[0])
    abfss = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{guid}/Files/{rel}"

    local = _download(abfss, base)
    fhash = _hash_file(local)
    file_key = f"{schema}/{fname}"
    if _already_processed(file_key, fhash):
        print(f"dropbox: {file_key} ({fhash[:8]}) already processed — skip + archive duplicate")
        _rm(local)
        _archive(guid, rel)                              # keep newfile/ clean
        return "skipped (already processed)"

    if ext in ("xlsx", "xls"):                               # one object per sheet
        import pandas as pd
        engine = "openpyxl" if ext == "xlsx" else "xlrd"
        _ensure_pkg(engine)
        sheets = pd.ExcelFile(local, engine=engine).sheet_names
        objs = [(f"{base}_{_norm_ident(s)}", {"file_abfss": abfss, "format": ext, "sheet": s})
                for s in sheets]
    else:                                                    # csv / txt -> one object
        objs = [(base, {"file_abfss": abfss, "format": "txt" if ext == "txt" else "csv"})]
    _rm(local)

    sid = _ensure_datasource(schema)
    loaded = 0
    for tbl, opts in objs:
        opts["landed"] = {"table": tbl}                      # clean <schema>.<tbl>, no dbo_ prefix
        oid = _norm_ident(f"dropbox_{schema}_{tbl}")
        keys_json, opts_json = json.dumps(["_row_hash"]), json.dumps(opts)
        _register_object(oid, sid, tbl, keys_json, opts_json)
        o = {"object_id": oid, "source_id": sid, "source_name": schema, "source_schema": None,
             "source_table": tbl, "connector": "file", "secret_name": None, "load_type": "append",
             "key_columns_json": keys_json, "source_options_json": opts_json}
        oj = json.dumps(o)
        bronze(run_id=run_id, object_json=oj)
        silver(run_id=run_id, object_json=oj)
        loaded += 1
        print(f"dropbox: loaded {schema}.{tbl}")

    _write_ledger(file_key, fhash, schema, loaded)
    _archive(guid, rel)
    print(f"dropbox: DONE {file_key} -> {loaded} table(s) in schema '{schema}'")
    return f"processed ({loaded} table{'s' if loaded != 1 else ''})"


def _list_newfile(guid):
    root = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{guid}/Files/newfile"
    out = []

    def walk(path):
        try:
            entries = notebookutils.fs.ls(path)
        except Exception:
            return
        for e in entries:
            (walk(e.path) if e.isDir else out.append(e.path))
    walk(root)
    return out


def _scan(run_id):
    """Process every file currently sitting under newfile/ (manual/scheduled batch, or a
    catch-up sweep). Per-file isolation: one bad file is reported, the rest still process."""
    guid = _filedrop_guid()
    files = _list_newfile(guid)
    print(f"dropbox scan: {len(files)} file(s) under newfile/")
    results = []
    for abfss in files:
        rel = abfss.split("/Files/", 1)[-1]
        try:
            results.append((rel, _run(rel, run_id) or "processed"))
        except Exception as e:
            files_put(f"_cp_err_dropbox_{_norm_ident(rel)}_{run_id}.txt", traceback.format_exc())
            results.append((rel, f"FAILED: {str(e)[:120]}"))
    ok = sum(1 for _, s in results if not s.startswith("FAILED"))
    print(f"dropbox scan DONE: {ok}/{len(results)} ok")
    for rel, s in results:
        print(f"  [{'OK ' if not s.startswith('FAILED') else 'ERR'}] {rel:<40} {s}")
    return results


def dropbox(file_path="", run_id="manual", **kw):
    """Process one dropped file (path relative to LH_Filedrop/Files, e.g. 'newfile/voc/x.xlsx').
    With no file_path, SCAN and process everything currently under newfile/."""
    try:
        return _run(file_path, run_id) if (file_path or "").strip() else _scan(run_id)
    except Exception:
        files_put(f"_cp_err_dropbox_{_norm_ident(file_path) or 'scan'}_{run_id}.txt", traceback.format_exc())
        raise
