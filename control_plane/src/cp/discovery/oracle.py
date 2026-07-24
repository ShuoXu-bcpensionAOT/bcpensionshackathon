"""Oracle object discovery — ALL_TABLES / ALL_CONSTRAINTS+ALL_CONS_COLUMNS (base tables + primary
keys). Oracle has no INFORMATION_SCHEMA; the ALL_* views show every table the read account can see.
Oracle-maintained schemas are excluded so discovery returns only application objects."""
from ..connectors.base import run_catalog_query
from . import discoverer, _candidates

_ORA_SYS_OWNERS = ("SYS", "SYSTEM", "XDB", "MDSYS", "CTXSYS", "OUTLN", "DBSNMP", "APPQOSSYS",
                   "ORDSYS", "ORDDATA", "WMSYS", "LBACSYS", "OLAPSYS", "GSMADMIN_INTERNAL",
                   "AUDSYS", "DVSYS", "ORACLE_OCM", "XS$NULL", "SYSBACKUP", "SYSKM", "SYSRAC")


@discoverer("oracle")
def _discover_oracle(ds):
    excl = ", ".join(f"'{o}'" for o in _ORA_SYS_OWNERS)
    tables = run_catalog_query(ds,
        f"SELECT OWNER AS t_s, TABLE_NAME AS t_n FROM ALL_TABLES WHERE OWNER NOT IN ({excl})").collect()
    pk = run_catalog_query(ds,
        "SELECT C.OWNER AS t_s, C.TABLE_NAME AS t_n, CC.COLUMN_NAME AS c_n, CC.POSITION AS ord "
        "FROM ALL_CONSTRAINTS C JOIN ALL_CONS_COLUMNS CC "
        "ON C.OWNER=CC.OWNER AND C.CONSTRAINT_NAME=CC.CONSTRAINT_NAME "
        "WHERE C.CONSTRAINT_TYPE='P'").collect()
    return _candidates(tables, pk)
