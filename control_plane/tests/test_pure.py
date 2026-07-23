"""Off-cluster unit tests for the pure modules (no Fabric/Spark needed)."""
from cp.naming import snake, _norm_ident, landed_table
from cp.dag import topo_levels


def test_snake():
    assert snake("ModifiedDate") == "modified_date"
    assert snake("SalesOrderID") == "sales_order_id"
    assert snake("already_snake") == "already_snake"


def test_norm_ident():
    assert _norm_ident("Stats Can") == "stats_can"
    assert _norm_ident("British Columbia!!") == "british_columbia"
    assert _norm_ident("  a--b__c ") == "a_b_c"


def test_landed_table_schema_enabled():
    o = {"source_name": "Stats Can", "source_schema": None, "source_table": "labour_force", "suffix": "bc"}
    assert landed_table(o) == ("stats_can", "dbo_labour_force_bc")
    o2 = {"source_name": "AdventureWorks", "source_schema": "Sales", "source_table": "Customer"}
    assert landed_table(o2) == ("adventureworks", "sales_customer")


def test_topo_levels_order():
    # a -> b -> c ; a -> c   => levels [[a],[b],[c]]
    levels = topo_levels(["a", "b", "c"], [("a", "b"), ("b", "c"), ("a", "c")])
    assert levels == [["a"], ["b"], ["c"]]


def test_topo_levels_cycle():
    import pytest
    with pytest.raises(ValueError):
        topo_levels(["a", "b"], [("a", "b"), ("b", "a")])
