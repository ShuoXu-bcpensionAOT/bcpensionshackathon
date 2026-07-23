"""Pure DAG helper — topological levelization for the gold build order. No dependencies."""


def topo_levels(nodes, edges):
    remaining, done = set(nodes), set()
    parents = {n: set() for n in nodes}
    for p, c in edges:
        if c in parents and p in remaining:
            parents[c].add(p)
    levels = []
    while remaining:
        ready = sorted([n for n in remaining if parents[n] <= done])
        if not ready:
            raise ValueError(f"cycle in gold DAG: {remaining}")
        levels.append(ready)
        done |= set(ready)
        remaining -= set(ready)
    return levels
