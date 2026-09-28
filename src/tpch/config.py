import os

CATALOG = os.getenv("TPCH_CATALOG", "workspace")
SOURCE = os.getenv("TPCH_SOURCE", "samples.tpch")
AS_OF_DATE = os.getenv("TPCH_AS_OF_DATE", "1998-08-02")
LAYERS = ("bronze", "silver", "gold")
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")


def fq(layer: str, table: str) -> str:
    """Fully qualified table name, e.g. workspace.bronze.orders"""
    return f"{CATALOG}.{layer}.{table}"