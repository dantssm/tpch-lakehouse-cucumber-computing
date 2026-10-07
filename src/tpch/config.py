import os

CATALOG = os.getenv("TPCH_CATALOG", "workspace")
SOURCE = os.getenv("TPCH_SOURCE", "samples.tpch")
AS_OF_DATE = os.getenv("TPCH_AS_OF_DATE", "1998-08-02")
LAYERS = ("bronze", "silver", "gold")
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
# Allowed ranges, taken from profiling the source (notebook 01)
DISCOUNT_MIN, DISCOUNT_MAX = 0, 0.10
TAX_MIN, TAX_MAX = 0, 0.08


def fq(layer: str, table: str) -> str:
    """Fully qualified table name, e.g. workspace.bronze.orders"""
    return f"{CATALOG}.{layer}.{table}"


def ensure_schemas(spark) -> None:
    """One-time setup: create the bronze/silver/gold schemas.
    Needs CREATE SCHEMA on the catalog, so only the catalog owner/admin runs this,
    once, outside the regular pipeline run. Everyone else only needs rights on
    their own schema (see the GRANT statements in the README)."""
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")