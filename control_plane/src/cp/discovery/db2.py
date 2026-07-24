"""IBM DB2 object discovery — SYSCAT catalog (base tables + primary keys). DB2 exposes SYSCAT
reliably across LUW versions (INFORMATION_SCHEMA is not guaranteed), so use it directly."""
from ..connectors.base import run_catalog_query
from . import discoverer, _candidates


@discoverer("db2")
def _discover_db2(ds):
    tables = run_catalog_query(ds,
        "SELECT RTRIM(TABSCHEMA) AS t_s, RTRIM(TABNAME) AS t_n FROM SYSCAT.TABLES "
        "WHERE TYPE='T' AND TABSCHEMA NOT LIKE 'SYS%'").collect()
    pk = run_catalog_query(ds,
        "SELECT RTRIM(K.TABSCHEMA) AS t_s, RTRIM(K.TABNAME) AS t_n, RTRIM(K.COLNAME) AS c_n, "
        "K.COLSEQ AS ord "
        "FROM SYSCAT.KEYCOLUSE K JOIN SYSCAT.TABCONST C "
        "ON K.CONSTNAME=C.CONSTNAME AND K.TABSCHEMA=C.TABSCHEMA AND K.TABNAME=C.TABNAME "
        "WHERE C.TYPE='P'").collect()
    return _candidates(tables, pk)
