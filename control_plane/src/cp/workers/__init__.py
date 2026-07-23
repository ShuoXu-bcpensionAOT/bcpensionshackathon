"""Worker entrypoints called by the (now thin) pipeline notebooks. Cell 3 of each worker
notebook is a single call into here, e.g. `workers.bronze(run_id=..., object_json=...)`."""
from .plan import plan
from .bronze import bronze
from .silver import silver
from .metadata import metadata
from .gold import gold

__all__ = ["plan", "bronze", "silver", "metadata", "gold"]
