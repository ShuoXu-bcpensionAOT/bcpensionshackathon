"""ANSI INFORMATION_SCHEMA object discovery — PostgreSQL & MySQL (base tables + primary keys).
Both expose the ANSI INFORMATION_SCHEMA views, so one discoverer serves both. (SQL Server has its
own module for its dialect quirks; Oracle/DB2 use their native catalogs.)"""
from ..connectors.base import run_catalog_query
from . import discoverer, _candidates

# system schemas to exclude (Postgres + MySQL). NB: on a multi-database MySQL server
# INFORMATION_SCHEMA spans every database — scope with a source-side filter if that over-enumerates.
_ANSI_SYS_SCHEMAS = ("pg_catalog", "information_schema", "mysql", "sys", "performance_schema")


@discoverer("postgresql", "mysql")
def _discover_ansi(ds):
    excl = ", ".join(f"'{s}'" for s in _ANSI_SYS_SCHEMAS)
    tables = run_catalog_query(ds,
        "SELECT TABLE_SCHEMA AS t_s, TABLE_NAME AS t_n FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA NOT IN ({excl})").collect()
    pk = run_catalog_query(ds,
        "SELECT KU.TABLE_SCHEMA AS t_s, KU.TABLE_NAME AS t_n, KU.COLUMN_NAME AS c_n, "
        "KU.ORDINAL_POSITION AS ord "
        "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC "
        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU "
        "ON TC.CONSTRAINT_NAME=KU.CONSTRAINT_NAME AND TC.CONSTRAINT_SCHEMA=KU.CONSTRAINT_SCHEMA "
        "WHERE TC.CONSTRAINT_TYPE='PRIMARY KEY'").collect()
    return _candidates(tables, pk)
