"""(Re)provision the feature (spoke) workspaces for one environment — the hub-and-spoke topology.

For each spoke in the manifest's `feature_workspaces`, creates (idempotently) its workspace
(`<base>-<name>-<env>`), a lakehouse (`LH_<name>`), and OneLake shortcuts pointing at THIS
environment's hub Gold — env-local, so a UAT spoke references UAT data, never DEV/PROD. Safe to
re-run (e.g. after the hub's Gold tables are first loaded). Spoke CONTENT (reports / semantic
models) is the domain team's own Fabric Git + deployment-pipeline track, not managed here.

    python cp_spokes.py HackathonShuo DEV
"""
import sys

import cp_bootstrap as B


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else "HackathonShuo"
    env = sys.argv[2] if len(sys.argv) > 2 else "DEV"
    t = B.token()
    hub_wid = B.ensure_workspace(t, f"{base}-{env}")   # find the hub (idempotent; won't recreate)
    B.provision_spokes(t, base, env, hub_wid)


if __name__ == "__main__":
    main()
