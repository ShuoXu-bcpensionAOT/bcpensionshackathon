"""Oracle connector — pure-Python `oracledb` thin mode by default; JDBC via mode='jdbc'."""
from . import ingest_connector
from .base import _resolve_conn, _opts, _ensure_pkg, _default_query, _dbapi_to_spark
from .jdbc import _ic_jdbc


@ingest_connector("oracle")
def _ic_oracle(o, user, password):
    """Oracle. Default: pure-Python `oracledb` THIN mode (pip-installed on demand, no Oracle
    client, no jar) reading driver-side. Opt in to distributed Spark JDBC (needs the ojdbc jar
    on an attached Fabric Environment) with connection_json.mode='jdbc'."""
    c, opts = _resolve_conn(o), _opts(o)
    if (c.get("mode") or "").lower() == "jdbc":
        return _ic_jdbc(o, user, password)
    user = c.get("user") or user
    password = c.get("password") or password
    oracledb = _ensure_pkg("oracledb")
    dsn = c.get("dsn") or (f"{c.get('host')}:{c.get('port', 1521)}/"
                           f"{c.get('service') or c.get('database') or o.get('database_name')}")
    cn = oracledb.connect(user=user, password=password, dsn=dsn)   # thin mode
    return _dbapi_to_spark(cn, opts.get("query") or _default_query(o))
