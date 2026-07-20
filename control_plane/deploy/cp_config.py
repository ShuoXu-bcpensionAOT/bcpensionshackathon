"""Config-as-code loader: control_plane/config/*.yml -> Fabric SQL Database (config_db).

Applies the promotion YAML into the target env's config SQL DB (full replace per
table, FK-safe order). Used during promotion; in DEV, users edit the tables
directly and cp_export_config snapshots them back to YAML.
Run: python scripts/cp_config.py
"""
import yaml

import cp_common as C
import cp_sqldb as S


def _load(name):
    with open(C.CONFIG_DIR / name, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or []


def main():
    gm = _load("gold_model.yml")
    data = {
        "datasource": _load("datasource.yml"),
        "model": gm.get("model", []),
        "source_object": _load("source_object.yml"),
        "dq_rule": _load("dq_rule.yml"),
        "cleanse_rule": _load("cleanse_rule.yml"),
        "gold_object": gm.get("gold_object", []),
        "gold_dependency": gm.get("gold_dependency", []),
        "steps": _load("steps.yml"),
        "pbi_dataset": _load("pbi_dataset.yml"),
    }

    cn = S.connect()
    S.ensure_schema(cn)
    cur = cn.cursor()
    # FK-safe full replace: delete children first, insert parents first.
    for t in reversed(S.LOAD_ORDER):
        cur.execute(f"DELETE FROM dbo.{t}")
    print(f"Loading config-as-code -> SQL Database '{S.CONFIG_DB_NAME}' ({C.WS_NAME}):")
    for t in S.LOAD_ORDER:
        cols = S.COLUMNS[t]
        rows = data[t]
        ph = ",".join(["?"] * len(cols))
        collist = ",".join(f"[{c}]" for c in cols)   # bracket-quote (reserved words)
        idn = t in S.IDENTITY_TABLES                 # preserve explicit ids from YAML
        if idn:
            cur.execute(f"SET IDENTITY_INSERT dbo.{t} ON")
        for r in rows:
            cur.execute(f"INSERT INTO dbo.{t} ({collist}) VALUES ({ph})",
                        *[r.get(c) for c in cols])
        if idn:
            cur.execute(f"SET IDENTITY_INSERT dbo.{t} OFF")
        print(f"  + {t:<16} {len(rows)} row(s)")
    cn.commit()
    cn.close()


if __name__ == "__main__":
    main()
