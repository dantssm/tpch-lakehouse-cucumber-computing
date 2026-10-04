"""Data validation. Each rule is a SQL query returning the number of violating rows: 0 = pass."""
from pyspark.sql import DataFrame, SparkSession, functions as F

from tpch.config import SOURCE, TABLES, fq


# Allowed values, taken from profiling the source (notebook 01, section 3)
ALLOWED = {
    ("customer", "c_mktsegment"): ["BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD", "AUTOMOBILE"],
    ("orders", "o_orderstatus"): ["F", "O", "P"],
    ("orders", "o_orderpriority"): ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"],
    ("lineitem", "l_returnflag"): ["N", "R", "A"],
    ("lineitem", "l_linestatus"): ["F", "O"],
    ("lineitem", "l_shipmode"): ["TRUCK", "FOB", "REG AIR", "AIR", "SHIP", "RAIL", "MAIL"],
}


def _checks() -> list[tuple[str, str, str]]:
    """(layer, rule, SQL returning the number of violating rows). Add silver/gold rules here."""
    o, c, li = fq("bronze", "orders"), fq("bronze", "customer"), fq("bronze", "lineitem")
    checks = [
        ("bronze", f"{t}: row count equals source",
         f"SELECT ABS((SELECT COUNT(*) FROM {SOURCE}.{t}) - (SELECT COUNT(*) FROM {fq('bronze', t)}))")
        for t in TABLES
    ]
    checks += [
        ("bronze", "orders: customer exists",
         f"SELECT COUNT(*) FROM {o} LEFT ANTI JOIN {c} ON o_custkey = c_custkey"),
        ("bronze", "lineitem: order exists",
         f"SELECT COUNT(*) FROM {li} LEFT ANTI JOIN {o} ON l_orderkey = o_orderkey"),
        ("bronze", "orders: has at least one line item",
         f"SELECT COUNT(*) FROM {o} LEFT ANTI JOIN {li} ON o_orderkey = l_orderkey"),
        ("bronze", "orders: key, customer and date not null",
         f"SELECT COUNT(*) FROM {o} WHERE o_orderkey IS NULL OR o_custkey IS NULL OR o_orderdate IS NULL"),
        ("bronze", "lineitem: quantity and price strictly positive",
         f"SELECT COUNT(*) FROM {li} WHERE NOT coalesce(l_quantity > 0 AND l_extendedprice > 0, false)"),
        # bounds from profiling (notebook 01, section 4): discount 0.00-0.10, tax 0.00-0.08
        ("bronze", "lineitem: discount between 0.00 and 0.10",
         f"SELECT COUNT(*) FROM {li} WHERE NOT coalesce(l_discount BETWEEN 0 AND 0.10, false)"),
        ("bronze", "lineitem: tax between 0.00 and 0.08",
         f"SELECT COUNT(*) FROM {li} WHERE NOT coalesce(l_tax BETWEEN 0 AND 0.08, false)"),
    ]
    checks += [
        ("bronze", f"{t}.{col}: value in allowed list",
         f"SELECT COUNT(*) FROM {fq('bronze', t)} WHERE NOT coalesce({col} IN ({', '.join(map(repr, vals))}), false)")
        for (t, col), vals in ALLOWED.items()
    ]

    s_orders = fq("silver", "orders")
    s_lineitem = fq("silver", "lineitem")
    s_customer = fq("silver", "customer")
    s_supplier = fq("silver", "supplier")
    s_nation = fq("silver", "nation")
    s_partsupp = fq("silver", "partsupp")

    checks += [
        ("silver", "orders: primary key not null and unique",
         f"SELECT COUNT(*) - COUNT(DISTINCT o_orderkey) FROM {s_orders}"),
        ("silver", "lineitem: ship_date before or equal receipt_date",
         f"SELECT COUNT(*) FROM {s_lineitem} WHERE l_shipdate > l_receiptdate"),
        ("silver", "lineitem: discount and tax within standard bounds [0, 1]",
         f"SELECT COUNT(*) FROM {s_lineitem} WHERE NOT (l_discount BETWEEN 0 AND 1 AND l_tax BETWEEN 0 AND 1)"),
        ("silver", "lineitem: quantity strictly positive",
         f"SELECT COUNT(*) FROM {s_lineitem} WHERE l_quantity <= 0"),
        ("silver", "orders: o_totalprice cannot be negative",
         f"SELECT COUNT(*) FROM {s_orders} WHERE o_totalprice < 0"),
        ("silver", "lineitem: ship_date cannot be earlier than parent order date",
         f"SELECT COUNT(*) FROM {s_lineitem} li JOIN {s_orders} o ON li.l_orderkey = o.o_orderkey WHERE li.l_shipdate < o.o_orderdate"),
        ("silver", "customer: every customer references a valid nation",
         f"SELECT COUNT(*) FROM {s_customer} LEFT ANTI JOIN {s_nation} ON c_nationkey = n_nationkey"),
        ("silver", "supplier: every supplier references a valid nation",
         f"SELECT COUNT(*) FROM {s_supplier} LEFT ANTI JOIN {s_nation} ON s_nationkey = n_nationkey"),
        ("silver", "lineitem: every part-supplier pair exists in partsupp",
         f"SELECT COUNT(*) FROM {s_lineitem} LEFT ANTI JOIN {s_partsupp} ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey")
    ]

    g_revenue = fq("gold", "revenue_metrics")
    g_customers = fq("gold", "customer_activity")
    g_suppliers = fq("gold", "top_suppliers")
    g_parts = fq("gold", "top_parts")

    checks += [
        ("gold", "reconciliation: gold net revenue matches silver sum within tolerance $0.01",
         f"""SELECT ABS(
                (SELECT COALESCE(SUM(net_revenue), 0) FROM {g_revenue}) - 
                (SELECT COALESCE(SUM(l_extendedprice * (1 - l_discount)), 0) FROM {s_lineitem})
             ) > 0.01 AS is_mismatch FROM (SELECT 1)"""),
        ("gold", "revenue_metrics: net_revenue is never negative",
         f"SELECT COUNT(*) FROM {g_revenue} WHERE net_revenue < 0"),
        ("gold", "top_suppliers: revenue contribution is strictly positive",
         f"SELECT COUNT(*) FROM {g_suppliers} WHERE total_revenue_contribution < 0"),
        ("gold", "top_parts: revenue contribution is strictly positive",
         f"SELECT COUNT(*) FROM {g_parts} WHERE total_revenue_contribution < 0"),
        ("gold", "customer_activity: trailing 12m flag has no nulls",
         f"SELECT COUNT(*) FROM {g_customers} WHERE is_active_trailing_12m IS NULL"),
        ("gold", "revenue_metrics: table is not empty",
         f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {g_revenue}")
    ]

    return checks


def run(spark: SparkSession, fail_on_error: bool = False) -> DataFrame:
    """Run all checks and return one row per rule. With fail_on_error=True, raise if any rule fails (for Jobs)."""
    rows = [(layer, rule, int(spark.sql(sql).first()[0])) for layer, rule, sql in _checks()]
    df = (spark.createDataFrame(rows, "layer string, rule string, violations long")
          .withColumn("passed", F.col("violations") == 0)
          .withColumn("checked_at", F.current_timestamp()))
    if fail_on_error and df.filter("NOT passed").count():
        raise AssertionError("Validation failed, see the results table")
    return df
