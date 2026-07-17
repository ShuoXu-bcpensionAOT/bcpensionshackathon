"""Config-as-code loader: control_plane/config/*.yml -> control tables (metadata lakehouse).

Local step (delta-rs, GUID paths). Git-promotable config; the Fabric engine
notebooks then read these control tables. Run: python scripts/cp_config.py
"""

from datetime import datetime, timezone

import pandas as pd
import yaml

import cp_common as C


def _load(name):
    with open(C.CONFIG_DIR / name, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or []


def main():
    tok = C.storage_token()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    datasource = _load("datasource.yml")
    source_object = _load("source_object.yml")
    dq_rule = _load("dq_rule.yml")
    gm = _load("gold_model.yml")

    def stamp(rows):
        for r in rows:
            r.setdefault("created_at", now)
            r["updated_at"] = now
        return rows

    tables = {
        "datasource": datasource,
        "source_object": source_object,
        "dq_rule": dq_rule,
        "model": gm.get("model", []),
        "gold_object": gm.get("gold_object", []),
        "gold_dependency": gm.get("gold_dependency", []),
    }
    print("Loading config-as-code -> control tables (metadata lakehouse):")
    for name, rows in tables.items():
        df = pd.DataFrame(stamp(rows))
        # normalize object/bool columns for arrow
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].astype("string")
        n = C.write_delta(C.LH["config"], name, df, tok)
        print(f"  + {name:<16} {n} row(s)")


if __name__ == "__main__":
    main()
