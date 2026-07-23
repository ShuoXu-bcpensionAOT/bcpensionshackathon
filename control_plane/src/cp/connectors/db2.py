"""IBM DB2 connector — pure-Python `ibm_db` by default; JDBC via mode='jdbc'."""
from . import ingest_connector
from .base import _resolve_conn, _opts, _ensure_pkg, _default_query, _dbapi_to_spark
from .jdbc import _ic_jdbc


@ingest_connector("db2")
def _ic_db2(o, user, password):
    """IBM DB2. Default: pure-Python `ibm_db` (pip-installed on demand; the wheel bundles the
    client) reading driver-side. Opt in to distributed Spark JDBC (needs the db2jcc jar on an
    attached Fabric Environment) with connection_json.mode='jdbc'."""
    c, opts = _resolve_conn(o), _opts(o)
    if (c.get("mode") or "").lower() == "jdbc":
        return _ic_jdbc(o, user, password)
    user = c.get("user") or user
    password = c.get("password") or password
    dbi = _ensure_pkg("ibm_db_dbi", "ibm_db")
    cs = (f"DATABASE={c.get('database') or o.get('database_name')};HOSTNAME={c.get('host')};"
          f"PORT={c.get('port', 50000)};PROTOCOL=TCPIP;UID={user};PWD={password};")
    return _dbapi_to_spark(dbi.connect(cs, "", ""), opts.get("query") or _default_query(o))
