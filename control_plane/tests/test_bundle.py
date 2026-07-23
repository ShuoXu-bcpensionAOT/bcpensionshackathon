"""Off-cluster tests for the bundler: the generated cp_framework cell must be syntactically
valid and expose every public symbol + registry entry the pipeline relies on."""
import ast

import cp_bundle as B


def test_bundle_parses_and_has_public_api():
    src = B.build()
    lines, syms = B.validate(src)         # raises if a public symbol or registry entry is missing
    assert lines > 500 and syms >= len(B.EXPECTED_PUBLIC)
    ast.parse(src)                        # redundant but explicit: valid Python


def test_generated_file_in_sync():
    """notebooks/cp_framework.py must equal a fresh bundle (regenerate before committing)."""
    generated = B.OUT.read_text(encoding="utf-8").rstrip("\n")
    fresh = B.build().rstrip("\n")
    assert generated == fresh, "cp_framework.py is stale — run `python deploy/cp_bundle.py`"


def test_every_connector_file_is_bundled():
    src = B.build()
    for name in ("_ic_jdbc", "_ic_odbc", "_ic_http", "_ic_oracle", "_ic_db2", "_ic_staged"):
        assert f"def {name}" in src, f"{name} missing from bundle"
